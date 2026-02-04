resource "aws_security_group" "fleetlm_ec2" {
  name        = "fleetlm-ec2-bench"
  description = "Security group for FleetLM EC2 instances - WIDE OPEN FOR BENCHMARKING ONLY"

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH access"
  }

  # Phoenix HTTP
  ingress {
    from_port   = 4000
    to_port     = 4000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Phoenix web server"
  }

  # EPMD (Erlang Port Mapper Daemon)
  ingress {
    from_port   = 4369
    to_port     = 4369
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "EPMD for Erlang clustering"
  }

  # Erlang distributed port range
  ingress {
    from_port   = 9100
    to_port     = 9155
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Erlang distributed ports"
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
    Name = "fleetlm-ec2-bench"
  }
}
