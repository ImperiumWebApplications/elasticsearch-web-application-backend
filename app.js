const express = require("express");
const mysql = require("mysql");
const dotenv = require("dotenv");
const cors = require("cors");
const { Client } = require("@elastic/elasticsearch");

dotenv.config();

const app = express();
const port = process.env.port;

app.use(cors());

// Connect to MySQL database
const connection = mysql.createConnection({
  host: process.env.host,
  user: process.env.user,
  password: process.env.password,
  database: process.env.database,
});

connection.connect((err) => {
  if (err) {
    console.error("Error connecting to MySQL: " + err.stack);
    return;
  }
  console.log("Connected to MySQL as id " + connection.threadId);
});

// Connect to Elasticsearch
const esClient = new Client({
  node: process.env.elasticsearch_node,
  auth: {
    username: process.env.elasticsearch_username,
    password: process.env.elasticsearch_password,
  },
});

// API endpoint to index data into Elasticsearch
app.get("/index", async (req, res) => {
  try {
    const [rows, fields] = await new Promise((resolve, reject) => {
      connection.query(
        "SELECT * FROM dpi_partnumberinfo",
        (err, rows, fields) => {
          if (err) reject(err);
          resolve([rows, fields]);
        }
      );
    });

    const documents = [];
    rows.forEach((row) => {
      documents.push({ index: { _index: "parts" } });
      documents.push(row);
    });

    esClient
      .bulk({ body: documents })
      .then((response) => {
        const indexed = response.items.map((item) => item.index);
        indexed.forEach((item, i) => {
          if (item.status && item.status >= 400) {
            console.error(
              `Error indexing document ${i}: ${JSON.stringify(item.error)}`
            );
          } else {
            console.log(`Indexed document ${i}`);
          }
        });
        res.send("Data indexed successfully");
      })
      .catch((error) => {
        console.error(error);
        res.status(500).send("Error indexing data");
      });
  } catch (err) {
    console.error(err);
    res.status(500).send("Error indexing data");
  }
});

// API endpoint to return suggestions based off the entered text
app.get("/suggestions", async (req, res) => {
  const query = req.query.q.toLowerCase();
  const { hits } = await esClient.search({
    index: "parts",
    body: {
      query: {
        wildcard: {
          partNumber: `*${query}*`,
        },
      },
    },
  });
  const suggestions = hits.hits.map((hit) => ({
    partNumber: hit._source.partNumber,
    part_id: hit._source.part_id,
  }));
  res.send(suggestions);
});

// API endpoint to return the product details
app.get("/product/:productID", (req, res) => {
  const productID = req.params.productID;
  const query = `
    SELECT dpi_partnumberinfo.partNumber, dpi_image_mapper.fileName, vcdb_brands.BrandName, vcdb_parts.PartTerminologyName, dpi_categories.categoryName, dpi_subcategories.SubCategoryName
    FROM dpi_partnumberinfo
    INNER JOIN vcdb_brands ON dpi_partnumberinfo.BrandID = vcdb_brands.BrandID
    INNER JOIN vcdb_parts ON dpi_partnumberinfo.PartTerminologyID = vcdb_parts.PartTerminologyID
    INNER JOIN dpi_categoryMapping on dpi_partnumberinfo.PartTerminologyID = dpi_categoryMapping.PartTerminologyID
    INNER JOIN dpi_categories ON dpi_categoryMapping.categoryID = dpi_categories.categoryID
    INNER JOIN dpi_subcategories ON dpi_categoryMapping.subcategoryID = dpi_subcategories.subcategoryID
    INNER JOIN dpi_image_mapper on dpi_partnumberinfo.part_id = dpi_image_mapper.part_id
    WHERE dpi_partnumberinfo.part_id = ?
  `;

  connection.query(query, [productID], (err, results, fields) => {
    if (err) throw err;
    if (results.length === 0) {
      res.status(404).send("Product not found");
    } else {
      res.send(results[0]);
    }
  });
});

// Start server
app.listen(port, () => {
  console.log("Server listening on port " + port);
});
