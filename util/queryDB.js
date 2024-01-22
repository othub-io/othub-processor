const othub_db = require("../config/othub_db");

module.exports = executeQuery = async (query, params, network, blockchain) => {
  return new Promise(async (resolve, reject) => {
    await othub_db.query(query, params, (error, results) => {
      if (error) {
        reject(error);
      } else {
        resolve(results);
      }
    });
  });
};
