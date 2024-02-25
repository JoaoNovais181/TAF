
O comportamento deve manter se o mesmo numa aplicação tolerante a faltas

Falta e Falha -> falha origina de uma falta (falta num componente leva a falha no sistema)

Tolerância a faltas requere replicação

- Qualquer componente do sistema é propensa a falhas
- Qualquer componente vital necessita de ser replicada
    * remover todos os pontos únicos de falha (SPOF - Single Point Of Failure)

Sistemas Distribuidos são adequados para replicação:

- Modularidade - como a aplicação é composta por varios modulos, é útil poder replicar
- Redundância Incremental - 
- Degradação Graciosa - como temos muita redundância, a falha de uma parte do sistema não leva a problemas
- Heterogeniedade
- Encapsulamento

A abordagem de replicação depende de:

- A mutabilidade do estado do componente
- O modelo de concorrência
- O tipo e numero de faltas (modelo de falta)
- A bias da carga de trabalho
