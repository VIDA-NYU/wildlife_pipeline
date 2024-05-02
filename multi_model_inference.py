#!/usr/bin/env python3
# coding: utf-8

from torch.utils.data import DataLoader, Dataset, random_split
from torchvision.transforms import transforms
import torch.nn as nn
import torch
from transformers import DistilBertModel
from PIL import Image
from transformers import DistilBertTokenizer
from torchvision import models
import torch.nn.functional as F
from io import BytesIO
import logging

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# We will first prepare the dataset for inference
class InferenceDataset(Dataset):
    def __init__(self, dataframe, tokenizer, max_len, transform, client, bucket_name):
        self.dataframe = dataframe
        self.tokenizer = tokenizer
        self.max_len = max_len
        self.transform = transform
        self.minio_client = client
        self.bucket_name = bucket_name

    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, idx):
        row = self.dataframe.iloc[idx]
        text = row['title']
        img_path = row['image_path']

        # Processing text
        encoding = self.tokenizer.encode_plus(
            text,
            add_special_tokens=True,
            max_length=self.max_len,
            return_token_type_ids=False,
            padding='max_length',
            return_attention_mask=True,
            return_tensors='pt',
            truncation=True
        )

        bucket, _, object_name = img_path.partition('/')
        if self.minio_client:
            image_data = self.minio_client.get_obj(self.bucket_name, object_name)
            image_bytes = image_data.read()
            image = Image.open(BytesIO(image_bytes)).convert("RGB")
        else:
            image = Image.open(img_path).convert("RGB")

        if self.transform:
            image = self.transform(image)

        return {
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'image': image
        }


text = DistilBertModel.from_pretrained('distilbert-base-uncased')

img = models.efficientnet_v2_m(pretrained=True)

class MultiHeadCrossAttention(nn.Module):
    def __init__(self, text_dim, image_dim, num_heads, hidden_dim, output_dim):
        super(MultiHeadCrossAttention, self).__init__()
        self.num_heads = num_heads
        self.head_dim = hidden_dim // num_heads

        # These linear layers project the inputs to multiple heads
        self.text_query = nn.Linear(text_dim, hidden_dim, bias=False)
        self.text_key = nn.Linear(text_dim, hidden_dim, bias=False)
        self.text_value = nn.Linear(text_dim, hidden_dim, bias=False)

        self.image_query = nn.Linear(image_dim, hidden_dim, bias=False)
        self.image_key = nn.Linear(image_dim, hidden_dim, bias=False)
        self.image_value = nn.Linear(image_dim, hidden_dim, bias=False)

        # Final projection layer
        self.out_proj = nn.Linear(hidden_dim, output_dim, bias=False)

    def forward(self, text_features, image_features):
        Q_text = self.text_query(text_features)
        K_text = self.text_key(text_features)
        V_text = self.text_value(text_features)

        Q_image = self.image_query(image_features)
        K_image = self.image_key(image_features)
        V_image = self.image_value(image_features)

        # Split the hidden dimension into num_heads
        Q_text = Q_text.view(Q_text.size(0), -1, self.num_heads, self.head_dim).transpose(1, 2)
        K_text = K_text.view(K_text.size(0), -1, self.num_heads, self.head_dim).transpose(1, 2)
        V_text = V_text.view(V_text.size(0), -1, self.num_heads, self.head_dim).transpose(1, 2)

        Q_image = Q_image.view(Q_image.size(0), -1, self.num_heads, self.head_dim).transpose(1, 2)
        K_image = K_image.view(K_image.size(0), -1, self.num_heads, self.head_dim).transpose(1, 2)
        V_image = V_image.view(V_image.size(0), -1, self.num_heads, self.head_dim).transpose(1, 2)

        # Calculate the attention scores
        attn_scores_text_image = torch.matmul(Q_text, K_image.transpose(-1, -2)) / (self.head_dim ** 0.5)
        attn_scores_image_text = torch.matmul(Q_image, K_text.transpose(-1, -2)) / (self.head_dim ** 0.5)

        # Normalize scores
        attn_probs_text_image = F.softmax(attn_scores_text_image, dim=-1)
        attn_probs_image_text = F.softmax(attn_scores_image_text, dim=-1)

        # Apply attention
        attn_output_text_image = torch.matmul(attn_probs_text_image, V_image)
        attn_output_image_text = torch.matmul(attn_probs_image_text, V_text)

        # Concatenate the results across the heads
        attn_output_text_image = attn_output_text_image.transpose(1, 2).contiguous().view(text_features.size(0), -1)
        attn_output_image_text = attn_output_image_text.transpose(1, 2).contiguous().view(image_features.size(0), -1)

        # Project to output dimension
        output_text_image = self.out_proj(attn_output_text_image)
        output_image_text = self.out_proj(attn_output_image_text)

        return output_text_image, output_image_text


class MultiModalModel(nn.Module):
    def __init__(self, num_labels):
        super(MultiModalModel, self).__init__()

        # Load pre-trained models
        self.bert = text
        self.resnet = img

        # Remove the final classification layer of ResNet
        self.resnet = nn.Sequential(*list(self.resnet.children())[:-1])
        self.mhca = MultiHeadCrossAttention(text_dim=768, image_dim=1280, num_heads=4, hidden_dim=512, output_dim=2048)


        self.classifier = nn.Sequential(
            nn.Linear(2816, 512),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(512, num_labels)
        )

    def forward(self, input_ids, attention_mask, image, device):
        # Forward pass through BERT
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        text_features = outputs['last_hidden_state'][:, 0, :]  # CLS token output as text feature

        # Forward pass through ResNet
        image_features = self.resnet(image)
        image_features = image_features.view(image_features.size(0), -1)  # Flatten the output

        if text_features.dim() == 2:
            text_features = text_features.unsqueeze(1)
        if image_features.dim() == 2:
            image_features = image_features.unsqueeze(1)

        attended_text, attended_image = self.mhca(text_features, image_features)

        attended_text = attended_text.squeeze(1)  # shape: [16, 768]
        attended_image = attended_image.squeeze(1) # shape: [16, 2048]

        self.image_projection = torch.nn.Linear(2048, 768).to(device)
        attended_image = self.image_projection(attended_image)
        combined_features = torch.cat((attended_text, attended_image), dim=-1)

        logits = self.classifier(combined_features)

        return logits


# Function to run inference
def run_inference(model, dataloader, device):
    model.eval()
    predictions = []

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            images = batch['image'].to(device)

            outputs = model(input_ids=input_ids, attention_mask=attention_mask, image=images, device=device)

            preds = torch.argmax(outputs, dim=1)
            predictions.extend(preds.cpu().numpy())

    return predictions

def get_inference_data(dataset, minio_client, bucket_name):

    MAX_LEN = 128
    BATCH_SIZE = 16

    tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased')
    transform = transforms.Compose([
        transforms.RandomHorizontalFlip(),
        transforms.RandomRotation(10),
        transforms.RandomResizedCrop(224, scale=(0.8, 1.0)),
        transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.1),
        transforms.ToTensor(),
    ])

    # Prepare dataset and dataloader for inference
    inference_dataset = InferenceDataset(
        dataframe=dataset,
        tokenizer=tokenizer,
        max_len=MAX_LEN,
        transform=transform,
        client=minio_client,
        bucket_name = bucket_name
    )

    return DataLoader(inference_dataset, batch_size=BATCH_SIZE, shuffle=False)
