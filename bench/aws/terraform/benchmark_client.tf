# Benchmark client instance for running k6 tests
# Runs in same VPC as Starcite nodes to eliminate WAN latency

resource "aws_instance" "benchmark_client" {
  ami           = data.aws_ami.amazon_linux_2023.id
  instance_type = "t3.micro"  # Just for running k6
  key_name      = local.key_name

  vpc_security_group_ids = [aws_security_group.benchmark_client.id]

  user_data                   = local.benchmark_client_user_data
  user_data_replace_on_change = true

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = {
    Name = "starcite-bench-client"
  }
}

# Security group for benchmark client
resource "aws_security_group" "benchmark_client" {
  name        = "starcite-bench-client"
  description = "Security group for k6 benchmark client"

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH access"
  }

  # Allow all outbound
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = {
    Name = "starcite-bench-client"
  }
}

# User data to install k6
locals {
  benchmark_client_user_data = <<-EOF
    #!/bin/bash
    set -e

    # Update system
    dnf update -y

    # Install k6
    wget -q https://github.com/grafana/k6/releases/download/v0.48.0/k6-v0.48.0-linux-amd64.tar.gz
    tar xzf k6-v0.48.0-linux-amd64.tar.gz
    mv k6-v0.48.0-linux-amd64/k6 /usr/local/bin/
    rm -rf k6-v0.48.0-linux-amd64*

    # Verify k6 installed
    k6 version

    echo "k6 installed successfully"
  EOF
}
