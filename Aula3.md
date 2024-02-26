# TF


## Modelo do sistema

- Dois processos A e B comunicam enviando e recebendo mensagens
- Nenhum dos processos pode falhar, mas o canal entre eles pode ter falhas momentaneas que resultam na perda de um subconjunto de mensagens

## Problema de Coordenação

- Desenvolver um protocolo onde 2 ações alfa e beta são possiveis, mas (i) ambos os processos tomam a mesma ação (ii) nenhum deles toma ambas as ações

Problema: Concordar num ataque simultâneo

Ambos atacam: Vitória
Só um ataca: Morto

```
 A                   B
 |         α         |
 |------------------>|
 |         α         |
 |<------------------|
 |         α         |
 |---------x-------->|  -> mensagem pode ser "roubada"
 |         ?         |
 |<------------------|
 |         α         |
 |------------------>|
 |                   |
----------------------- t = tomada da decisão -> output α/β
 |                   |
α/β                 α/β
```

No máximo usam M mensagens

**Este problema NESTE modelo, não tem solução**

## Síncrono ou Assíncrono

- Modelo síncrono - existem e sabemos quais são os tempos máximos para algo acontecer
- Modelo assíncrono - não há um tempo máximo garantido para algo acontecer, inevitavelmente acontecerá mas não se sabe quando

## Modelo de Sistema A

conjunto de processos P1,.., Pn. Cada processo tem um identificador único

Não há adversário

Todos os processos começam a executar ao mesmo tempo mas sem assunções síncronas

Tempo leva 2N

### Problema de eleição

Desenvolver um protocolo para eleger um líder único inevitavelmente conhecido por todos

## Modelo de Sistema S

Processos levam no máximo t<sub>p</sub> por passo e mensagens levam no maximo t<sub>m</sub> para ser entregues

Conseguimos melhor tempo que 2N
