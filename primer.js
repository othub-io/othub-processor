require("dotenv").config();
const mysql = require("mysql");
const othubdb_connection = mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  password: process.env.DBPASSWORD,
  database: process.env.OTHUB_DB,
});

const prime_db = async () =>{
  try {
    const wallet_array = JSON.parse(process.env.WALLET_ARRAY);
    for (i = 0; i < wallet_array.length; i++) {
        query = `INSERT INTO txn_header (txn_id, progress, approver, api_key, request, network, app_name, txn_description, txn_data, ual, keywords, state, txn_hash, txn_fee, trac_fee, epochs, receiver) VALUES (UUID(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;
        await othubdb_connection.query(
          query,
          [
            "COMPLETE",
            wallet_array[i].public_key,
            'primer',
            'Create-n-Transfer',
            'otp::testnet',
            'primer',
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
          ],
          function (error, results, fields) {
            if (error) throw error;
          }
        );

        query = `INSERT INTO txn_header (txn_id, progress, approver, api_key, request, network, app_name, txn_description, txn_data, ual, keywords, state, txn_hash, txn_fee, trac_fee, epochs, receiver) VALUES (UUID(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;
        await othubdb_connection.query(
          query,
          [
            "COMPLETE",
            wallet_array[i].public_key,
            'primer',
            'Create-n-Transfer',
            'otp::mainnet',
            'primer',
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
          ],
          function (error, results, fields) {
            if (error) throw error;
          }
        );
    }
    console.log('done')
  } catch (e) {
    console.log(e)
  }
}

prime_db()
