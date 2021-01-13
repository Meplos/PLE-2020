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

app.get("/word_pop", (req, res) => {
  let result;
  client.table("gresse_word_pop").scan(
    {
      batch: 100,
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

app.get("/hashtag_pop/:tag", (req, res) => {
  const rows = [];
  const tag = req.params.tag;
  client
    .table("gresse_hashtag_pop")
    .row(tag)
    .get("days", (err, chunks) => {
      if (err) {
        console.log("ERR " + err);
        res.send(err.status);
      } else {
        console.log("Size : " + chunks.length);
        res.status(200).send(chunks);
      }
    });
  /*scanner.get((err, chunks) => {
    console.error("ERR " + err);
    console.log("Chunks : " + chunks);
  });*/
});

app.get("/", (req, res) => res.status(400).send("Welcome 👌!"));

app.listen(PORT, () => console.log("Server Listening"));

function groupArrayOfObjects(list, key) {
  return list.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
}

/** /word_pop result
 * 
 *  {
 *      word: [
 *          {
 *          key: 'virus',
            column: 'number:Mar 1',
            timestamp: 1610460353756,
            '$': '16' 
            }, {
              *          key: 'virus',
            column: 'number:Mar 2',
            timestamp: 1610460353756,
            '$': '163' 
   
            }
 *      ], 
 *    confinement:  ...
 * }
 * 
 * 
 */