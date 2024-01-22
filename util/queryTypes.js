const queryDB = require("./queryDB");

const queryTypes = [
  {
    name: "queryDB",
    getData: (query, params, network, blockchain) => queryDB(query, params, network, blockchain),
  }
];

module.exports = {
  queryDB: function queryDB() {
    return queryTypes[0];
  }
};
