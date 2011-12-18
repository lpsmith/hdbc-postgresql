{-# LANGUAGE ForeignFunctionInterface #-} 
-----------------------------------------------------------------------------
-- |
-- Module      :  Database.HDBC.PostgreSQL.LargeObjects
-- Copyright   :  (c) 2011 Leon P Smith
-- License     :  LGPL
--
-- Maintainer  :  leon@melding-monads.com
--
-----------------------------------------------------------------------------


module Database.HDBC.PostgreSQL.LargeObjects (loImport, loExport, Oid) where

#include <libpq/libpq-fs.h>

import Foreign.C
import Foreign.Ptr

import Database.HDBC.PostgreSQL.Types
import Database.HDBC.PostgreSQL.Utils
import qualified Database.HDBC.PostgreSQL.ConnectionImpl as Impl

type Oid = CUInt

toMaybeOid :: Oid -> Maybe Oid
toMaybeOid oid = case oid of
                   0 -> Nothing
                   _ -> Just oid

loImport :: Impl.Connection -> FilePath -> IO (Maybe Oid)
loImport connection filepath
    = withConnLocked (Impl.conn connection) $ \c -> do
        withCString filepath $ \f -> do
          toMaybeOid `fmap` c_lo_import c f

loExport :: Impl.Connection -> Oid -> FilePath -> IO CInt
loExport connection oid filepath
    = withConnLocked (Impl.conn connection) $ \c -> do
        withCString filepath $ \f -> do
          c_lo_export c oid f

foreign import ccall unsafe "libpq-fs.h lo_import"
    c_lo_import :: Ptr CConn -> CString -> IO Oid

foreign import ccall unsafe "libpq-fs.h lo_export"
    c_lo_export :: Ptr CConn -> Oid -> CString -> IO CInt
