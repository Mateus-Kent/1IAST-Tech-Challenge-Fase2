Galera boa noite, seguinte vcs que estao no review, aqui to fazendo o comeco da parte de aws. subindo estancia e tals.
No momento vamos usar s3 e to passsando texto de exemplo aqui pra ficar mais facil, mas esse metodo obviamente
vai depender de ter o aws CLI instalado na maquina e configurado com a sua maquina(Conta)

Tambem pesquisando podemos fazer direto tela tambem, acho que por hora fica mais facil e compartilhamos as contas entre si.
Vi que temos 100 dolares mesmo logando padrao e conseguimos usar o S3 de boa.


Sobre o que falei de compartilhar a conta, esquece, uma pessoa cria e usamos o AWS IAM, mais facil

provider "aws" {
  region = "us-east-1" # ou sa-east-1
}

# 1. Cria o Bucket principal
resource "aws_s3_bucket" "datalake" {
  bucket = "tech-challenge-fase2-orlando-dados" #orlando e o nome que ta no meu pc relaxem
}

# aqui e pra criar pastas dentro do bucket principal
resource "aws_s3_object" "data_raw" {
  bucket = aws_s3_bucket.datalake.id
  key    = "data/raw/"
}

resource "aws_s3_object" "data_streaming" {
  bucket = aws_s3_bucket.datalake.id
  key    = "data/streaming_raw/"
}

resource "aws_s3_object" "layer_bronze" {
  bucket = aws_s3_bucket.datalake.id
  key    = "layers/bronze/"
}

resource "aws_s3_object" "layer_silver" {
  bucket = aws_s3_bucket.datalake.id
  key    = "layers/silver/"
}

resource "aws_s3_object" "layer_gold" {
  bucket = aws_s3_bucket.datalake.id
  key    = "layers/gold/"
}

