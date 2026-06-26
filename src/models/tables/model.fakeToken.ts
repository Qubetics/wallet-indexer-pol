import { DataTypes, Model, Optional } from "sequelize";
import db from "../../config/db";

interface FakeCoinAttributes {
    id?: number | null;
    token_address: string;
    name: string;
    symbol?: string | null;
    decimals?: number | null;
    verified?: string | null;
    category?: string | null;
    created_at?: Date | null;
    updated_at?: Date | null;
}

interface FakeCoinCreationAttributes extends Optional<FakeCoinAttributes, "symbol" | "decimals" | "verified" | "category"> {}

interface FakeCoinInstance
    extends Model<FakeCoinAttributes, FakeCoinCreationAttributes>,
        FakeCoinAttributes {}

const dataObj = {
    id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
        autoIncrement: true,
    },
    token_address: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: "Token address",
    },
    name: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
    },
    symbol: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
    },
    decimals: {
        type: DataTypes.INTEGER,
        allowNull: true,
        defaultValue: null,
    },
    verified: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
    },
    category: {
        type: DataTypes.STRING,
        allowNull: true,
        comment: "Category",
        defaultValue: null,
    },
};

const dataObjIndex = {
    indexes: [
        {
            unique: true,
            fields: ["token_address"],
        }
    ],
    timestamps: true,
    createdAt: "created_at",
    updatedAt: "updated_at",
};

const FakeCoinsModel = db.db_write.define<FakeCoinInstance>(
    "fake_coins",
    dataObj,
    dataObjIndex
);

FakeCoinsModel.sync({ alter: true });

export default FakeCoinsModel;
