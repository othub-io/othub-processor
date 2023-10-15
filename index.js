require("dotenv").config();
const mysql = require("mysql");
const othubdb_connection = mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  password: process.env.DBPASSWORD,
  database: process.env.OTHUB_DB,
});

const DKGClient = require("dkg.js");
const OT_NODE_TESTNET_PORT = process.env.OT_NODE_TESTNET_PORT;
//const OT_NODE_MAINNET_PORT = process.env.OT_NODE_MAINNET_PORT;

const testnet_node_options = {
  endpoint: process.env.OT_NODE_HOSTNAME,
  port: OT_NODE_TESTNET_PORT,
  useSSL: true,
  maxNumberOfRetries: 100,
};

// const mainnet_node_options = {
//   endpoint: process.env.OT_NODE_HOSTNAME,
//   port: OT_NODE_MAINNET_PORT,
//   useSSL: true,
//   maxNumberOfRetries: 100,
// };

const testnet_dkg = new DKGClient(testnet_node_options);
//const mainnet_dkg = new DKGClient(mainnet_node_options);

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

async function uploadData(data) {
  try {
    query = `UPDATE txn_header SET progress = ?, approver = ? WHERE txn_id = ?`;
    await othubdb_connection.query(
      query,
      ["PROCESSING", data.approver, data.txn_id],
      function (error, results, fields) {
        if (error) throw error;
      }
    );

    let dkg_txn_data = JSON.parse(data.txn_data);
    if (!dkg_txn_data["@context"]) {
      dkg_txn_data["@context"] = "https://schema.org";
    }

    const wallet_array = JSON.parse(process.env.WALLET_ARRAY);
    let index = wallet_array.findIndex(
      (obj) => obj.public_key == data.approver
    );

    console.log(
      `Using ${wallet_array[index].name} wallet ${wallet_array[index].public_key} for next asset creation on ${data.network}.`
    );

    if (data.network === "otp::testnet") {
      let dkg_create_result = await testnet_dkg.asset
        .create(
          {
            public: dkg_txn_data,
          },
          {
            epochsNum: data.epochs,
            maxNumberOfRetries: 30,
            frequency: 2,
            contentType: "all",
            keywords: data.keywords,
            blockchain: {
              name: data.network,
              publicKey: data.approver,
              privateKey: wallet_array[index].private_key,
            },
          }
        )
        .then((result) => {
          //console.log(JSON.stringify(result))
          return result;
        })
        .catch(async (error) => {
          console.log(error);
          console.log(
            `Create for Create n Transfer request failed. Setting back to pending...`
          );
          query = `UPDATE txn_header SET progress = ? WHERE  txn_id = ?`;
          await othubdb_connection.query(
            query,
            ["PENDING", data.txn_id],
            function (error, results, fields) {
              if (error) throw error;
            }
          );
          return;
        });

      console.log(
        `Created UAL ${dkg_create_result.UAL} with ${wallet_array[index].name} wallet ${wallet_array[index].public_key}.`
      );
      console.log(`Transfering to ${data.receiver}...`);

      await testnet_dkg.asset
        .transfer(dkg_create_result.UAL, data.receiver, {
          epochsNum: data.epochs,
          maxNumberOfRetries: 30,
          frequency: 2,
          contentType: "all",
          keywords: data.keywords,
          blockchain: {
            name: data.network,
            publicKey: data.approver,
            privateKey: wallet_array[index].private_key,
          },
        })
        .then(async (result) => {
          console.log(
            `Transfered ${dkg_create_result.UAL} to ${data.receiver} with ${wallet_array[index].name} wallet ${wallet_array[index].public_key}.`
          );

          query = `UPDATE txn_header SET progress = ?, ual = ?, state = ? WHERE  txn_id = ?`;
          await othubdb_connection.query(
            query,
            [
              "COMPLETE",
              dkg_create_result.UAL,
              dkg_create_result.publicAssertionId,
              data.txn_id,
            ],
            function (error, results, fields) {
              if (error) throw error;
            }
          );

          return result;
        })
        .catch(async (error) => {
          console.log(error);
          console.log(`Create for Create n Transfer request failed.`);
          query = `UPDATE txn_header SET progress = ? WHERE  txn_id = ?`;
          await othubdb_connection.query(
            query,
            ["TRANSFER-FAILED", data.txn_id],
            function (error, results, fields) {
              if (error) throw error;
            }
          );
          return;
        });
    }

    return;
  } catch (error) {
    throw new Error("Upload failed: " + error.message);
  }
}

async function getPendingUploadRequests() {
  try {
    console.log(`Checking for transactions to process...`);
    sqlQuery = "select * FROM txn_header where progress = ? and network = ?";
    params = ["PROCESSING", "otp::testnet"];
    testnet_processing_count = await getOTHubData(sqlQuery, params)
      .then((results) => {
        //console.log('Query results:', results);
        return results;
        // Use the results in your variable or perform further operations
      })
      .catch((error) => {
        console.error("Error retrieving data:", error);
      });

    // sqlQuery = "select * FROM txn_header where progress = ? and network = ?";
    // params = ["PROCESSING", "otp::mainnet"];
    // mainnet_processing_count = await getOTHubData(sqlQuery, params)
    //   .then((results) => {
    //     //console.log('Query results:', results);
    //     return results;
    //     // Use the results in your variable or perform further operations
    //   })
    //   .catch((error) => {
    //     console.error("Error retrieving data:", error);
    //   });

    testnet_pending_count =
      Number(process.env.WALLET_COUNT) -
      Number(testnet_processing_count.length);
    // mainnet_pending_count =
    //   Number(process.env.WALLET_COUNT) -
    //   Number(mainnet_processing_count.length);

    sqlQuery =
      "select * FROM txn_header where progress = ? and network = ? ORDER BY updated_at DESC LIMIT ?";
    params = ["PENDING", "otp::testnet", 1];
    testnet_request = await getOTHubData(sqlQuery, params)
      .then((results) => {
        //console.log('Query results:', results);
        return results;
        // Use the results in your variable or perform further operations
      })
      .catch((error) => {
        console.error("Error retrieving data:", error);
      });

    // sqlQuery =
    //   "select * FROM txn_header where progress = ? and network = ? ORDER BY created_at DESC LIMIT ?";
    // params = ["PENDING", "otp::mainnet", 1];
    // mainnet_request = await getOTHubData(sqlQuery, params)
    //   .then((results) => {
    //     //console.log('Query results:', results);
    //     return results;
    //     // Use the results in your variable or perform further operations
    //   })
    //   .catch((error) => {
    //     console.error("Error retrieving data:", error);
    //   });

    const wallet_array = JSON.parse(process.env.WALLET_ARRAY);
    let available_testnet_wallets = [];
    for (i = 0; i < wallet_array.length; i++) {
      query = `select * from txn_header where request = 'Create-n-Transfer' AND approver = ? AND network = ? order by updated_at desc LIMIT 1`;
      params = [wallet_array[i].public_key, "otp::testnet"];
      testnet_last_processed = await getOTHubData(query, params)
        .then((results) => {
          //console.log('Query results:', results);
          return results;
          // Use the results in your variable or perform further operations
        })
        .catch((error) => {
          console.error("Error retrieving data:", error);
        });

      if (Number(testnet_last_processed.length) !== 0) {
        if (
          testnet_last_processed[0].progress !== "PROCESSING"
        ) {
          available_testnet_wallets.push(wallet_array[i]);
        }
      }
    }

    // const available_mainnet_wallets = [];
    // for (i = 0; i < wallet_array.length; i++) {
    //   query = `select * from txn_header where request = 'Create-n-Transfer' AND approver = ? AND network = ? order by updated_at desc LIMIT 1`;
    //   params = [wallet_array[i].public_key, "otp::mainnet"];
    //   mainnet_last_processed = await getOTHubData(query, params)
    //     .then((results) => {
    //       //console.log('Query results:', results);
    //       return results;
    //       // Use the results in your variable or perform further operations
    //     })
    //     .catch((error) => {
    //       console.error("Error retrieving data:", error);
    //     });

    //   if (Number(mainnet_last_processed.length) !== 0) {
    //     if (
    //       mainnet_last_processed[0].progress !== "PROCESSING" &&
    //       mainnet_last_processed[0].progress !== "PENDING"
    //     ) {
    //       available_mainnet_wallets.push(wallet_array[i]);
    //     }
    //   }
    // }

    if (Number(available_testnet_wallets.length) === 0) {
      testnet_request = [];
    } else if (Number(testnet_request) !== 0) {
      testnet_request[0].approver = available_testnet_wallets[0].public_key;
    } else {
    }

    console.log(
      `Testnet has ${available_testnet_wallets.length} open positions.`
    );

    // if (Number(available_mainnet_wallets.length) === 0) {
    //   mainnet_request = [];
    // } else if (Number(mainnet_request) !== 0) {
    //   console.log(
    //     `Mainnet has ${available_mainnet_wallets.length} open positions.`
    //   );
    //   mainnet_request[0].approver = available_mainnet_wallets[0].public_key;
    // } else {
    // }

    pending_requests = {
      testnet_requests: testnet_request,
      //mainnet_requests: mainnet_request,
    };

    return pending_requests;
  } catch (error) {
    throw new Error("Error fetching pending requests: " + error.message);
  }
}

async function processPendingUploads() {
  try {
    setTimeout(processPendingUploads, process.env.CYCLE_TIME_SEC * 1000);
    const pending_requests = await getPendingUploadRequests();

    const testnet_promises = pending_requests.testnet_requests.map((request) =>
      uploadData(request)
    );

    // const mainnet_promises = pending_requests.mainnet_requests.map(
    //   async (request) => uploadData(request)
    // );

    const concurrentUploads = 10;
    await Promise.all(testnet_promises.slice(0, concurrentUploads));

    // await Promise.all(mainnet_promises.slice(0, concurrentUploads));
  } catch (error) {
    console.error("Error processing pending uploads:", error);
  }
}

processPendingUploads();
