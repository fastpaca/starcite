terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Fetch default VPC
data "aws_vpc" "default" {
  default = true
}

# Fetch default subnets
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Get latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Create SSH key pair if ssh_public_key is provided
resource "aws_key_pair" "starcite" {
  count      = var.ssh_public_key != "" ? 1 : 0
  key_name   = "starcite-bench-key"
  public_key = var.ssh_public_key

  tags = {
    Name = "starcite-bench-key"
  }
}

# Determine which key to use
locals {
  key_name = var.key_name != "" ? var.key_name : (var.ssh_public_key != "" ? aws_key_pair.starcite[0].key_name : null)
}

# User data script to install Docker and mount instance store
locals {
  user_data = <<-EOF
    #!/bin/bash
    set -e

    # Update system
    dnf update -y

    # Install Docker
    dnf install -y docker

    # Mount instance store NVMe for WAL storage (if available)
    if lsblk | grep -q nvme1n1; then
      echo "Mounting instance store NVMe..."
      mkfs.ext4 -F /dev/nvme1n1
      mkdir -p /mnt/nvme
      mount /dev/nvme1n1 /mnt/nvme
      chmod 777 /mnt/nvme
      echo "Instance store mounted at /mnt/nvme"
    fi

    # Start and enable Docker
    systemctl start docker
    systemctl enable docker

    # Add ec2-user to docker group
    usermod -aG docker ec2-user

    # Wait for Docker to be ready
    sleep 5

    echo "Docker installed successfully"
  EOF
}

# EC2 instances
resource "aws_instance" "starcite" {
  count = var.instance_count

  ami           = data.aws_ami.amazon_linux_2023.id
  instance_type = var.instance_type
  key_name      = local.key_name

  vpc_security_group_ids = [aws_security_group.starcite_ec2.id]

  user_data                   = local.user_data
  user_data_replace_on_change = true

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = {
    Name  = "starcite-bench-${count.index + 1}"
    Index = count.index
  }
}
