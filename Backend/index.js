const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const hbase = require("hbase");

const PORT = 3000;
const client = hbase({ host: "127.0.0.1", port: 8080 });

const app = express();

app.use(bodyParser.urlencoded());
app.use(bodyParser.json());
app.use(cors());

app.get("/", (req, res) => res.status(400).send("Welcome 👌!"));

app.get("/word_pop", (req, res) => {
  let result;
  client.table("gresse_word_pop").scan(
    {
      startRow: "confinement",
      maxVersions: 1,
    },
    function (err, rows) {
      result = rows;
      const grouped = groupArrayOfObjects(result, "key");
      console.log(grouped);
      res.status(200).send(grouped);
      if (err) {
        res.sendStatus(500);
      }
    }
  );
});

app.listen(PORT, () => console.log("Server Listening"));

function groupArrayOfObjects(list, key) {
  return list.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
}
