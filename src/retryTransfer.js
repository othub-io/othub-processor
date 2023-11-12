require("dotenv").config();
const mysql = require("mysql");
const DKGClient = require("dkg.js");

const othubdb_connection = mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  password: process.env.DBPASSWORD,
  database: process.env.OTHUB_DB,
});

const OT_NODE_TESTNET_PORT = process.env.OT_NODE_TESTNET_PORT;
const OT_NODE_MAINNET_PORT = process.env.OT_NODE_MAINNET_PORT;

const testnet_node_options = {
  endpoint: process.env.OT_NODE_HOSTNAME,
  port: OT_NODE_TESTNET_PORT,
  useSSL: true,
  maxNumberOfRetries: 100,
};

const mainnet_node_options = {
  endpoint: process.env.OT_NODE_HOSTNAME,
  port: OT_NODE_MAINNET_PORT,
  useSSL: true,
  maxNumberOfRetries: 100,
};

const testnet_dkg = new DKGClient(testnet_node_options);
const mainnet_dkg = new DKGClient(mainnet_node_options);

const wallet_array = JSON.parse(process.env.WALLET_ARRAY);

function executeOTHubQuery(query, params) {
    return new Promise((resolve, reject) => {
      othubdb_connection.query(query, params, (error, results) => {
        if (error) {
          reject(error);
        } else {
          resolve(results);
        }
      });
    });
  }
  
  async function getOTHubData(query, params) {
    try {
      const results = await executeOTHubQuery(query, params);
      return results;
    } catch (error) {
      console.error("Error executing query:", error);
      throw error;
    }
  }
  
module.exports={
    retryTransfer: async function retryTransfer(data) {
        try {
          let index = wallet_array.findIndex(
            (obj) => obj.public_key == data.approver
          );
      
          query = `UPDATE txn_header SET updated_at = ? WHERE txn_id = ?`;
          params = [new Date(), data.txn_id];
          await getOTHubData(query, params)
            .then((results) => {
              //console.log('Query results:', results);
              return results;
              // Use the results in your variable or perform further operations
            })
            .catch((error) => {
              console.error("Error retrieving data:", error);
            });
      
          console.log(
            `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Retrying transfer for UAL ${data.ual} on ${data.network}.`
          );
      
          let dkg = testnet_dkg
          if(data.network === "otp::testnet" ){
            dkg = testnet_dkg
          }
      
          if(data.network === "otp::mainnet" && data.api_key === process.env.MASTER_KEY){
            dkg = mainnet_dkg
          }
      
          await dkg.asset
              .transfer(data.ual, data.receiver, {
                maxNumberOfRetries: 30,
                frequency: 2,
                contentType: "all",
                blockchain: {
                  name: data.network,
                  publicKey: wallet_array[index].public_key,
                  privateKey: wallet_array[index].private_key,
                },
              })
              .then(async (result) => {
                console.log(
                  `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Transfered ${data.ual} to ${data.receiver}.`
                );
      
                query = `UPDATE txn_header SET progress = ?, ual = ?, state = ? WHERE  txn_id = ?`;
                params = ["COMPLETE", data.ual, data.publicAssertionId, data.txn_id];
                await getOTHubData(query, params)
                  .then((results) => {
                    //console.log('Query results:', results);
                    return results;
                    // Use the results in your variable or perform further operations
                  })
                  .catch((error) => {
                    console.error("Error retrieving data:", error);
                  });
      
                return result;
              })
              .catch(async (error) => {
                error_obj = {
                    error: error,
                    index: index,
                    request: "Transfer",
                    ual: data.ual,
                    network: data.network,
                    receiver: data.receiver,
                  };
                  throw new Error(JSON.stringify(error_obj));
              });
        } catch (error) {
            let message = JSON.parse(error.message)
            await handleErrors.handleError(message)
        }
      }
}
