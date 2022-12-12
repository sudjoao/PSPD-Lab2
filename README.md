# PSPD-Lab2
## Pré requisitos
- Estar em uma máquina linux
- Instalar as seguintes dependencias:
    - [librdkafka](https://github.com/edenhill/librdkafka)
    - [pkg-config](https://www.freedesktop.org/wiki/Software/pkg-config/)
    - [libc](https://www.gnu.org/software/libc/)
    - [libglib-2.0.0](https://zoomadmin.com/HowToInstall/UbuntuPackage/libglib2.0-0)
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