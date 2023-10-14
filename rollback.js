require("dotenv").config();
const mysql = require("mysql");
const othubdb_connection = mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  password: process.env.DBPASSWORD,
  database: process.env.OTHUB_DB,
});

const rollback = async () =>{
  try {
    query = `UPDATE txn_header set progress = 'PENDING' where progress='PROCESSING' OR progress='FAILED`;
        await othubdb_connection.query(
          query,
          [],
          function (error, results, fields) {
            if (error) throw error;
          }
        );
    console.log('done')
  } catch (e) {
    console.log(e)
  }
}

rollback()
