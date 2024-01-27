const express = require('express');
const mysql = require('mysql');

const app = express();
app.use(express.json());

// Configuração da conexão com o banco de dados MySQL
const connection = mysql.createConnection({
  host: '189.27.188.112',
  port: '3306',
  user: 'root',
  password: '',
  database: 'cnpj',
});

connection.connect(err => {
  if (err) {
    console.error('Erro ao conectar no banco de dados: ', err);
    return;
  }
  console.log('Conexão bem sucedida ao banco de dados MySQL!');
});



// Função para buscar sócios
function buscarSocios(cnpjBasico) {
    return new Promise((resolve, reject) => {
        const querySocios = `
            SELECT nome_socio, qualificacao_socio, data_entrada_sociedade
            FROM socios_original
            WHERE cnpj_basico = ?;
        `;

        connection.query(querySocios, [cnpjBasico], (error, socios) => {
            if (error) {
                return reject(error);
            }
            resolve(socios);
        });
    });
}

// Função para executar consultas ao banco de dados
function connectionQuery(query, params) {
    return new Promise((resolve, reject) => {
        connection.query(query, params, (error, results) => {
            if (error) {
                return reject(error);
            }
            resolve(results);
        });
    });
}

// Rota da API para buscar informações completas do CNPJ
app.get('/api/completo/:cnpj', async (req, res) => {
    const cnpjCompleto = req.params.cnpj;
    const cnpjBasico = cnpjCompleto.slice(0, 8);

    const queryEmpresaFilial = `
        SELECT 
            est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv, emp.razao_social, 
            est.data_inicio_atividades, emp.porte_empresa, emp.natureza_juridica,
            sim.opcao_mei, sim.opcao_simples, sim.data_opcao_mei, sim.data_opcao_simples,
            emp.capital_social_str, est.matriz_filial, est.situacao_cadastral, est.data_situacao_cadastral,
            est.correio_eletronico, est.ddd1, est.telefone1, est.ddd2, est.telefone2,
            est.tipo_logradouro, est.logradouro, est.numero, est.complemento, est.bairro, est.cep, 
            mun.descricao as municipio, est.uf, 
            cnae.descricao as descricao_cnae_fiscal,
            GROUP_CONCAT(DISTINCT cnae_sec.descricao SEPARATOR '; ') as descricao_cnae_fiscal_secundaria
        FROM estabelecimento est
        JOIN empresa emp ON est.cnpj_basico = emp.cnpj_basico
        LEFT JOIN simples sim ON est.cnpj_basico = sim.cnpj_basico
        JOIN municipio mun ON est.municipio = mun.codigo
        LEFT JOIN cnae ON est.cnae_fiscal = cnae.codigo
        LEFT JOIN cnae cnae_sec ON FIND_IN_SET(cnae_sec.codigo, est.cnae_fiscal_secundaria)
        WHERE est.cnpj_basico = SUBSTRING(?, 1, 8)
        GROUP BY est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv
        ORDER BY est.matriz_filial ASC;
    `;

    try {
        const resultadosEmpresaFilial = await connectionQuery(queryEmpresaFilial, [cnpjBasico]);
        const socios = await buscarSocios(cnpjBasico);

        const resposta = resultadosEmpresaFilial.map(empresaFilial => ({
            ...empresaFilial,
            socios: socios
        }));

        res.json(resposta);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Rota da API para buscar informações associadas ao cnpj_basico
app.get('/api/basico/:cnpjBasico', (req, res) => {
    const cnpjBasico = req.params.cnpjBasico;

    const queryBasico = `
        SELECT 
            est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv, est.nome_fantasia, 
            est.data_inicio_atividades, emp.razao_social, emp.porte_empresa, 
            sim.data_opcao_simples, sim.data_opcao_mei
        FROM estabelecimento est
        JOIN empresa emp ON est.cnpj_basico = emp.cnpj_basico
        LEFT JOIN simples sim ON est.cnpj_basico = sim.cnpj_basico
        WHERE est.cnpj_basico = ?;
    `;

    const startTime = Date.now();

    connection.query(queryBasico, [cnpjBasico], (error, results) => {
        const endTime = Date.now();
        const duration = endTime - startTime;

        if (error) {
            res.status(500).json({ error: error.message });
            return;
        }

        if (results.length > 0) {
            res.json({
                data: results,
                queryTime: `${duration} ms`
            });
        } else {
            res.status(404).json({ message: 'Estabelecimentos não encontrados.' });
        }
    });
});

// Defina a porta e inicie o servidor
const PORT = process.env.PORT || 3010;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
