{-
Copyright (C) 2005-2007 John Goerzen <jgoerzen@complete.org>

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
-}

module Database.HDBC.PostgreSQL.ConnectionImpl where

import qualified Database.HDBC.Types as Types
import Database.HDBC.ColTypes as ColTypes
import Data.ByteString ( ByteString )
import Database.HDBC.PostgreSQL.Types (Conn)

data Connection = 
    Connection {
                disconnect :: IO (),
                commit :: IO (),
                rollback :: IO (),
                runRaw :: ByteString -> IO (),
                run :: ByteString -> [Types.SqlValue] -> IO Integer,
                prepare :: ByteString -> IO Types.Statement,
                clone :: IO Connection,
                hdbcDriverName :: ByteString,
                hdbcClientVer :: ByteString,
                proxiedClientName :: ByteString,
                proxiedClientVer :: ByteString,
                dbServerVer :: ByteString,
                dbTransactionSupport :: Bool,
                getTables :: IO [ByteString],
                describeTable :: ByteString -> IO [(ByteString, ColTypes.SqlColDesc)],
                conn :: Conn
               }

instance Types.IConnection Connection where
  disconnect = disconnect
  commit = commit
  rollback = rollback
  runRaw = runRaw
  run = run
  prepare = prepare
  clone = clone
  hdbcDriverName = hdbcDriverName
  hdbcClientVer = hdbcClientVer
  proxiedClientName = proxiedClientName
  proxiedClientVer = proxiedClientVer
  dbServerVer = dbServerVer
  dbTransactionSupport = dbTransactionSupport
  getTables = getTables
  describeTable = describeTable
