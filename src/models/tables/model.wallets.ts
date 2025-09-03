import { DataTypes, Model, Optional } from "sequelize";
import { WalletInterface } from "../interfaces/index";
import db from "../../config/db";
import CoinsModel from "./model.coins";
interface WalletCreationModel extends Optional<WalletInterface, "wallet_id"> { }
interface WalletInstance
  extends Model<WalletInterface, WalletCreationModel>,
  WalletInterface { }

const dataObj = {
  wallet_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
    primaryKey: true,
    autoIncrement: true,
  },
  user_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
  },
  wallet_name: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  wallet_address: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  coin_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
  },
  coin_family: {
    type: DataTypes.TINYINT,
    allowNull: false,
  },
  balance: {
    type: DataTypes.DOUBLE,
    allowNull: false,
  },
  // default_wallet: {
  //   type: DataTypes.TINYINT,
  //   allowNull: true,
  // },
  status: {
    type: DataTypes.TINYINT,
    allowNull: true,
  },
  sort_order: {
    type: DataTypes.INTEGER,
    allowNull: true,
  },
  // is_private_wallet: {
  //   type: DataTypes.TINYINT,
  //   allowNull: true,
  // },
};

const dataObjIndex = {
  indexes: [
    {
      unique: false,
      fields: ["user_id"],
    },
    {
      unique: false,
      fields: ["wallet_name"],
    },
    {
      unique: false,
      fields: ["wallet_address"],
    },
    {
      unique: false,
      fields: ["coin_id"],
    },
    {
      unique: false,
      fields: ["coin_family"],
    },
    {
      unique: false,
      fields: ["balance"],
    },
    {
      unique: false,
      fields: ["status"],
    },
    {
      unique: false,
      fields: ["created_at"],
    },
  ],
};

const WalletModel = db.db_write.define<WalletInstance>(
  "wallets",
  dataObj,
  dataObjIndex
);

WalletModel.belongsTo(CoinsModel, {
  foreignKey: "coin_id",
  targetKey: "coin_id",
  as: "coin_data"
});



export default WalletModel;