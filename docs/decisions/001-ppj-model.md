# 001 - Modelo baseado em PPJ

## Contexto

O projeto precisava de uma métrica central que fosse:

- simples de explicar
- comparável entre contextos
- robusta com poucos dados
- adequada para uma ferramenta analítica de leitura rápida

## Decisão

Usar PPJ (Pontos por Jogo) como métrica central do modelo.

## Motivo

- Fácil interpretação
- Comparável entre times e contextos
- Funciona bem com baixa amostragem
- Permite combinar histórico, forma e projeção sem perder legibilidade
- Facilita a leitura do dashboard por usuários não técnicos

## Consequências

### Positivas
- modelo mais transparente
- explicação mais simples
- manutenção mais fácil
- boa aderência a cenários com poucos jogos disputados

### Negativas
- menos sofisticado que abordagens baseadas em xG ou modelos probabilísticos mais ricos
- depende mais da qualidade do contexto e dos ajustes auxiliares
- pode simplificar excessivamente algumas nuances do desempenho real

## Status

Aceita

## Observação

O projeto utiliza PPJ como núcleo da leitura analítica, mas admite evolução futura com camadas complementares de modelagem.