require("dotenv").config();
const queryTypes = require("../util/queryTypes");
const queryDB = queryTypes.queryDB();
const DKGClient = require("dkg.js");
const handleErrors = require("./handleErrors.js");

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

module.exports = {
  retryTransfer: async function retryTransfer(data) {
    try {
      let query = `UPDATE txn_header SET progress = ? WHERE txn_id = ?`;
      let params = ["RETRYING-TRANSFER", data.txn_id];
      await queryDB
        .getData(query, params)
        .then((results) => {
          //console.log('Query results:', results);
          return results;
          // Use the results in your variable or perform further operations
        })
        .catch((error) => {
          console.error("Error retrieving data:", error);
        });

      let index = wallet_array.findIndex(
        (obj) => obj.public_key == data.approver
      );

      console.log(
        `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Retrying transfer for UAL ${data.ual} on ${data.blockchain}.`
      );

      let dkg = testnet_dkg;
      let environment;
      if (data.blockchain === "otp:20430" || data.blockchain === "gnosis:10200" || data.blockchain === "base:84532") {
        dkg = testnet_dkg;
        environment = "testnet";
      }

      if (
        (data.blockchain === "otp:2043" || data.blockchain === "gnosis:100" || data.blockchain === "base:8453") &&
        data.api_key === process.env.MASTER_KEY
      ) {
        dkg = mainnet_dkg;
        environment = "mainnet";
      }

      let bchain = {
        name: data.blockchain,
        publicKey: wallet_array[index].public_key,
        privateKey: wallet_array[index].private_key,
        handleNotMinedError: true,
      }

      let dkg_options = {
        environment: environment,
        epochsNum: data.epochs,
        maxNumberOfRetries: 30,
        frequency: 2,
        contentType: "all",
        keywords: data.keywords,
        blockchain: bchain,
      }

      if(data.bid){
        dkg_options.tokenAmount = data.bid
      }

      if(data.paranet_ual){
        dkg_options.bchain.paranetUAL = data.paranet_ual
      }

      await dkg.asset
        .transfer(data.ual, data.receiver, dkg_options)
        .then(async (result) => {
          console.log(
            `${wallet_array[index].name} wallet ${wallet_array[index].public_key}: Transfered ${data.ual} to ${data.receiver}.`
          );

          query = `UPDATE txn_header SET progress = ? WHERE progress in(?,?) AND approver = ?`;
          params = [
            "COMPLETE",
            "PROCESSING",
            "RETRYING-TRANSFER",
            wallet_array[index].public_key,
          ];
          await queryDB
            .getData(query, params)
            .then((results) => {
              //console.log('Query results:', results);
              return results;
              // Use the results in your variable or perform further operations
            })
            .catch((error) => {
              console.error("Error retrieving data:", error);
            });
        })
        .catch(async (error) => {
          error_obj = {
            error: error.message,
            index: index,
            request: "Transfer",
            ual: data.ual,
            blockchain: data.blockchain,
            receiver: data.receiver,
          };
          throw new Error(JSON.stringify(error_obj));
        });
      return;
    } catch (error) {
      let message = JSON.parse(error.message);
      await handleErrors.handleError(message);
    }
  },
};
