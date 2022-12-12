# PSPD-Lab2
## Pré requisitos
- Estar uma máquina linux
- Ter o C instalado na sua máquina
- Ter o kafka instalado na sua máquina
- Ter o libglib2.0 instalado na sua máquina
## Como rodar
1. Gerar os arquivos executáveis dos producers e consumers com o seguinte comando:
```
make producer
make consumer
```

2. Rodar até 6 consumers com o comando
```
./consumer getting_started.ini
```


3. Rodar o producer passando o arquivo de configuração e o caminho para o txt com as palavras
```
./producer getting_started.ini test.txt
```