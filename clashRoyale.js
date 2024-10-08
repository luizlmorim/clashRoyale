// 1. Calcule a porcentagem de vitórias e derrotas utilizando a carta 26000008 ocorridas entre 22:00 do dia 31/12/2020 às 10:00 no dia 01/01/2021.
db.batalhas.aggregate([
  {
    // Filtra as batalhas dentro do intervalo de timestamps fornecido
    $match: {
      battleTime: {
        $gte: "2020-01-01T10:00:00Z", 
        $lte: "2020-12-31T22:00:00Z"  
      },
      // Verifica se a carta específica foi usada
      $or: [
        {"winner.cards.list": {"$in": [26000008]}}, // ID da carta no vencedor
        {"loser.cards.list": {"$in": [26000008]}}   // ID da carta no perdedor
      ]
    }
  },
  {
    // Agrupa os resultados para contar vitórias e derrotas
    $group: {
      _id: null,
      totalBattles: { $sum: 1 },
      winsWithCard: {
        $sum: {
          $cond: [
            { $in: [26000008, "$winner.cards.list"] }, // Se a carta está no vencedor
            1,
            0
          ]
        }
      },
      lossesWithCard: {
        $sum: {
          $cond: [
            { $in: [26000008, "$loser.cards.list"] }, // Se a carta está no perdedor
            1,
            0
          ]
        }
      }
    }
  },
  {
    // Calcula as porcentagens de vitórias e derrotas
    $project: {
      _id: 0,
      totalBattles: 1,
      winPercentage: { 
        $multiply: [
          { $divide: ["$winsWithCard", "$totalBattles"] }, 
          100 
        ]
      },
      lossPercentage: { 
        $multiply: [
          { $divide: ["$lossesWithCard", "$totalBattles"] }, 
          100 
        ]
      }
    }
  }
])


// 2. Liste os decks completos que produziram mais de 70% de vitórias ocorridas entre 22:00 do dia 31/12/2020 às 10:00 no dia 01/01/2021.
db.batalhas.aggregate([
  {
    // Filtra as batalhas dentro do intervalo de timestamps fornecido
    $match: {
      battleTime: {
        $gte: "2020-12-31T22:00:00Z",
        $lt: "2021-01-01T10:00:00Z"
      }
    }
  },
  {
    // Agrupa as batalhas pelos decks completos usados pelos vencedores (winner.cards.list).
    $group: {
      _id: "$winner.cards.list",
      totalBattles: { $sum: 1 },
      wins: { $sum: { $cond: [ { $gt: ["$winner.crowns", "$loser.crowns"] }, 1, 0 ] } }
    }
  },
  {
    // Calcula a taxa de vitória (winRate) para cada deck.
    $project: {
      winRate: { $multiply: [ { $divide: ["$wins", "$totalBattles"] }, 100 ] },
      deck: "$_id"
    }
  },
  {
    // Filtra apenas os decks que têm uma taxa de vitória superior a 70%.
    $match: {
      winRate: { $gt: 70 }
    }
  }
])


// 3. Calcule a quantidade de derrotas utilizando o combo de cartas (26000003, 26000004, 26000006, 26000008, 26000022, 27000004, 28000001, 28000002) ocorridas entre 22:00 do dia 31/12/2020 às 10:00 no dia 01/01/2021.
db.batalhas.aggregate([
  {
    // Filtra as batalhas dentro do intervalo de timestamps fornecido
    $match: {
      battleTime: {
        $gte: "2020-12-31 22:00:00+00:00",
        $lte: "2021-01-01 10:00:00+00:00"
      },
      // Verificar se o perdedor usou exatamente o combo de cartas fornecido
      "loser.cards.list": {
        $size: 8,  // Garantir que o perdedor usou exatamente 8 cartas
        $all: [26000003, 26000004, 26000006, 26000008, 26000022, 27000004, 28000001, 28000002]
      }
    }
  },
  {
    // Contar o número de derrotas
    $count: "numDerrotas"
  }
])


// 4. Calcule a quantidade de vitórias envolvendo a carta 26000008 nos casos em que o vencedor possui 15% menos troféus do que o perdedor e o perdedor derrubou ao menos duas torres do adversário. 
db.batalhas.aggregate([
  {
    // Filtra as batalhas onde o vencedor tem a carta 26000008
    $match: {
      "winner.cards.list": { $in: [26000008] }
    }
  },
  {
    // Calcula a diferença percentual de troféus entre vencedor e perdedor
    $addFields: {
      trophyDifferencePercent: {
        $subtract: [
          1,
          { $divide: [ "$winner.startingTrophies", "$loser.startingTrophies" ] }
        ]
      }
    }
  },
  {
    // Filtra as batalhas onde o vencedor tem 15% menos troféus e o perdedor derrubou 2 ou mais torres
    $match: {
      trophyDifferencePercent: { $gte: 0.05 },
      "loser.crowns": { $gte: 2 }
    }
  },
  {
    // Conta o número de vitórias que satisfazem os critérios
    $count: "vitoriasComCarta26000008"
  }
])


// 5. Liste o combo de cartas com 8 cartas que produziram mais de 70% de vitórias ocorridas entre 22:00 do dia 31/12/2020 às 10:00 no dia 01/01/2021.
db.batalhas.aggregate([
  {
    // Filtra as batalhas no intervalo de tempo especificado
    $match: {
      battleTime: {
        $gte: "2020-12-31T22:00:00Z",
        $lte: "2021-01-01T10:00:00Z"
      }
    }
  },
  {
    // Agrupa as batalhas por combo de cartas do vencedor
    $group: {
      _id: {
        cards: "$winner.cards.list"
      },
      wins: { $sum: 1 }, // Conta o número de vitórias por combo de cartas
      totalBattles: { $sum: 1 } // Conta o total de batalhas
    }
  },
  {
    // Calcula a porcentagem de vitórias
    $addFields: {
      winPercentage: { $multiply: [{ $divide: ["$wins", "$totalBattles"] }, 100] }
    }
  },
  {
    // Filtra apenas combos com mais de 70% de vitórias
    $match: {
      winPercentage: { $gt: 70 }
    }
  },
  {
    // Ordena por porcentagem de vitórias decrescente
    $sort: {
      winPercentage: -1
    }
  },
  {
    // Limita o resultado aos principais combos de cartas
    $limit: 10
  }
]).pretty();


// 6. Calcule a porcentagem de vitórias e derrotas para jogadores com mais de 5000 troféus entre 22:00 do dia 31/12/2020 às 10:00 no dia 01/01/2021.
db.batalhas.aggregate([
  {
    $match: {
      "battleTime": {
        $gte: "2020-12-31T22:00:00Z",
        $lte: "2021-01-01T10:00:00Z"
      },
      $or: [
        { "winner.startingTrophies": { $gte: 5000 } },
        { "loser.startingTrophies": { $gte: 5000 } }
      ]
    }
  },
  {
    $facet: {
      totalMatches: [
        {
          $count: "total"
        }
      ],
      winnerMatches: [
        {
          $match: {
            "winner.startingTrophies": { $gte: 5000 }
          }
        },
        {
          $count: "winnerTotal"
        }
      ],
      loserMatches: [
        {
          $match: {
            "loser.startingTrophies": { $gte: 5000 }
          }
        },
        {
          $count: "loserTotal"
        }
      ]
    }
  },
  {
    $project: {
      total: { $arrayElemAt: ["$totalMatches.total", 0] },
      winnerTotal: { $arrayElemAt: ["$winnerMatches.winnerTotal", 0] },
      loserTotal: { $arrayElemAt: ["$loserMatches.loserTotal", 0] },
      winPercentage: {
        $multiply: [
          { $divide: [{ $arrayElemAt: ["$winnerMatches.winnerTotal", 0] }, { $arrayElemAt: ["$totalMatches.total", 0] }] },
          100
        ]
      },
      lossPercentage: {
        $multiply: [
          { $divide: [{ $arrayElemAt: ["$loserMatches.loserTotal", 0] }, { $arrayElemAt: ["$totalMatches.total", 0] }] },
          100
        ]
      }
    }
  }
]);


// 7. Calcule a porcentagem de vitórias em partidas onde o jogador utilizou cartas com custo de elixir médio acima de 4 contra jogadores com custo de elixir médio abaixo de 2, entre 22:00 do dia 31/12/2020 às 10:00 no dia 01/01/2021.
db.batalhas.aggregate([
  {
    // Filtrar pelo intervalo de tempo entre 22:00 de 31/12/2020 e 10:00 de 01/01/2021
    $match: {
      battleTime: {
        $gte: "2020-12-31T22:00:00Z",
        $lte: "2021-01-01T10:00:00Z"
      },
      // Filtrar por elixir médio dos jogadores
      "winner.elixir.average": { $gt: 4 },
      "loser.elixir.average": { $lt: 2 }
    }
  },
  {
    // Contar o número total de partidas que atendem aos critérios
    $group: {
      _id: null,
      totalBattles: { $sum: 1 },
      victories: {
        // Somar as vitórias
        $sum: {
          $cond: [{ $gt: ["$winner.crowns", "$loser.crowns"] }, 1, 0]
        }
      }
    }
  },
  {
    // Calcular a porcentagem de vitórias
    $project: {
      _id: 0,
      totalBattles: 1,
      victories: 1,
      winPercentage: {
        $multiply: [{ $divide: ["$victories", "$totalBattles"] }, 100]
      }
    }
  }
])


// 8. Liste os decks com o nível total maior que 100 que produziram mais de 70% de vitórias entre 22:00 do dia 31/12/2020 às 10:00 no dia 01/01/2021.
db.decks.aggregate([
  {
    $match: {
      battleTime: {
        $gte: ISODate("2020-12-31T22:00:00Z"), 
        $lt: ISODate("2021-01-01T10:00:00Z")
      }
    }
  },
  {
    $group: {
      _id: "$winner.tag", // Agrupar pelo tag do vencedor
      totalWins: { $sum: 1 }, // Contar o número total de vitórias
      totalLevel: { $avg: "$winner.totalcard.level" }, // Calcular o nível total médio
      totalBattles: { $sum: { $cond: [{ $eq: ["$winner.tag", "$winner.tag"] }, 1, 0] } } // Contar todas as batalhas que envolvem o vencedor
    }
  },
  {
    $match: {
      totalWins: { $gt: 0 }, // Garantir que haja vitórias
      totalLevel: { $gt: 100 }, // Filtrar níveis totais maiores que 100
      totalBattles: { $gt: 0 } // Garantir que haja batalhas
    }
  },
  {
    $project: {
      tag: "$_id",
      winRate: { $multiply: [{ $divide: ["$totalWins", "$totalBattles"] }, 100] }, // Calcular a taxa de vitórias em porcentagem
      totalLevel: 1
    }
  },
  {
    $match: {
      winRate: { $gt: 70 } // Filtrar taxa de vitórias maior que 70%
    }
  }
]);
