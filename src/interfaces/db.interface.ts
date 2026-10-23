import { Sequelize } from "sequelize";

export interface DBInterface {
  db_write: Sequelize;
  db_read: Sequelize;
  connectionWrite: string;
  connectionRead: string;
}
