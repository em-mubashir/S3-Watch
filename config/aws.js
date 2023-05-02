const pool = require("./db");
const xml2js = require("xml2js");

async function getAwsConfig(channelName) {
  const [rows] = await pool.query(
    `SELECT * FROM deliverychannels where CHANNELNAME = "${channelName}"`
  );
  // Find the S3 channel
  const s3Channel = rows.find((channel) => channel.CHANNELTYPE === "s3");

  if (!s3Channel) {
    throw new Error("S3 channel not found");
  }

  const configXML = s3Channel.CONFIGXML;
  let config;

  xml2js.parseString(configXML, (err, result) => {
    if (err) {
      console.error(err);
      throw new Error("Failed to parse config XML");
    } else {
      config = {
        bucketName: result.config.Param.find(
          (param) => param.$.Name === "bucket-name"
        ).$.Value,
        destinationFolder: result.config.Param.find(
          (param) => param.$.Name === "destination-folder"
        ).$.Value,
        awsRegion: result.config.Param.find(
          (param) => param.$.Name === "region"
        ).$.Value,
        accessKeyId: result.config.Param.find(
          (param) => param.$.Name === "access-key"
        ).$.Value,
        secretAccessKey: result.config.Param.find(
          (param) => param.$.Name === "secret-key"
        ).$.Value,
      };
    }
  });

  return config;
}

module.exports = { getAwsConfig };
