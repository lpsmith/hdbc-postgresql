{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ViewPatterns             #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Database.HDBC.PostgreSQL.Notification
-- Copyright   :  (c) 2011 Leon P Smith
-- License     :  BSD3
--
-- Maintainer  :  leon@melding-monads.com
-- Stability   :  experimental
--
-----------------------------------------------------------------------------

module Database.HDBC.PostgreSQL.Notification 
     ( Notification(..)
     , getNotification
     ) where

#include <libpq-fe.h>

import Foreign
import Foreign.C.Types
import Foreign.C.String

import System.Posix.Types ( CPid, Fd(..) )
import Control.Concurrent ( threadWaitRead )

import Database.HDBC.PostgreSQL.Types
import Database.HDBC.PostgreSQL.Utils
import qualified Database.HDBC.PostgreSQL.ConnectionImpl as Impl
import qualified Data.ByteString as B

data Notification  = Notification
                      { notificationPid     :: CPid
                      , notificationChannel :: B.ByteString
                      , notificationData    :: B.ByteString
                      }

getNotification :: Impl.Connection -> IO Notification
getNotification (Impl.conn -> connection0) = do
    lockConn connection0
    loop connection0
  where
    -- now, I believe the only ways that this code throws an exception is:
    --    1.  lockConn/unlockConn when we are blocked on a GC'd MVar
    --    2.  threadWaitRead,  if we properly handle disconnects
    --    3.  and if we raise it ourself
    -- If 1 happens, then it doesn't matter if the MVar is locked or not, 
    -- and if 2 or 3 happens then we should have unlocked the connection.
    loop connection = do
        mptr <- withRawConn connection c_PQnotifies
        if mptr == nullPtr 
          then do 
                  mfd <- withRawConn connection c_PQsocket 
                  unlockConn connection
                  case mfd of
                    -1 -> do 
                             fail errmsg
                    fd -> do
                             threadWaitRead (Fd fd)
                             lockConn connection
                             _ <- withRawConn connection c_PQconsumeInput
                                 -- FIXME? error handling
                             loop connection
          else do
                  unlockConn connection
                  res <- peek mptr
                  c_PQfreemem mptr
                  return res
    errmsg = "Database.HDBC.PostgreSQL.Notification.getNotification: \
             \failed to fetch file descriptor"

-- based on the haskell libpq bindings by Grant Monroe, BSD licensed

#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)
instance Storable Notification where
  sizeOf _ = #{size PGnotify}

  alignment _ = #{alignment PGnotify}

  peek ptr = do
      relname <- B.packCString =<< #{peek PGnotify, relname} ptr
      extra   <- B.packCString =<< #{peek PGnotify, extra} ptr
      be_pid  <- fmap f $ #{peek PGnotify, be_pid} ptr
      return $ Notification be_pid relname extra
      where
        f :: CInt -> CPid
        f = fromIntegral

  poke ptr (Notification b a c) =
      B.useAsCString a $ \a' ->
        B.useAsCString c $ \c' ->
            do #{poke PGnotify, relname} ptr a'
               #{poke PGnotify, be_pid}  ptr (fromIntegral b :: CInt)
               #{poke PGnotify, extra}   ptr c'


foreign import ccall unsafe "libpq-fe.h PQsocket"
    c_PQsocket :: Ptr WrappedCConn -> IO CInt

foreign import ccall unsafe "libpq-fe.h PQconsumeInput"
    c_PQconsumeInput :: Ptr WrappedCConn ->IO CInt

foreign import ccall unsafe "libpq-fe.h PQnotifies"
    c_PQnotifies :: Ptr WrappedCConn -> IO (Ptr Notification)

foreign import ccall unsafe "libpq-fe.h PQnotifies"
    c_PQfreemem :: Ptr Notification -> IO ()
