output "instance_ips" {
  description = "Public IP addresses of EC2 instances"
  value       = aws_instance.starcite[*].public_ip
}

output "instance_private_ips" {
  description = "Private IP addresses of EC2 instances"
  value       = aws_instance.starcite[*].private_ip
}

output "cluster_nodes" {
  description = "Cluster nodes string for CLUSTER_NODES env var"
  value       = join(",", [for ip in aws_instance.starcite[*].private_ip : "starcite@${ip}"])
}

output "ssh_commands" {
  description = "SSH commands to connect to instances"
  value       = [for idx, ip in aws_instance.starcite[*].public_ip : "ssh ec2-user@${ip}"]
}

output "benchmark_client_ip" {
  description = "Public IP of benchmark client instance"
  value       = aws_instance.benchmark_client.public_ip
}

output "benchmark_client_private_ip" {
  description = "Private IP of benchmark client instance"
  value       = aws_instance.benchmark_client.private_ip
}

output "benchmark_client_ssh" {
  description = "SSH command for benchmark client"
  value       = "ssh ec2-user@${aws_instance.benchmark_client.public_ip}"
}
