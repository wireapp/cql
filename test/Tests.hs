{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE OverloadedStrings #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Tests where

import Control.Applicative hiding (many)
import Control.Arrow
import Database.CQL.Protocol
import Database.CQL.Protocol.Internal
import Data.Decimal
import Data.Int
import Data.IP
import Data.Maybe
import Data.Serialize
import Data.Time
import Data.Time.Clock.POSIX
import Data.UUID
import Test.QuickCheck
import Test.Tasty
import Test.Tasty.QuickCheck
import Prelude

import qualified Data.ByteString.Lazy as LB
import qualified Data.Text            as T

tests :: TestTree
tests = testGroup "Codec"
    [ testProperty "V2: getValue . putValue = id" (getPutIdentity :: Val V2 -> Property)
    , testProperty "V3: getValue . putValue = id" (getPutIdentity :: Val V3 -> Property)
    , testProperty "V4: getValue . putValue = id" (getPutIdentity :: Val V4 -> Property)
    , testProperty "toCql . fromCql = id" toCqlFromCqlIdentity
    , testGroup "Integrals"
        [ testProperty "Int Codec"     $ integralCodec (elements [-512..512]) IntColumn CqlInt
        , testProperty "BigInt Codec"  $ integralCodec (elements [-512..512]) BigIntColumn CqlBigInt
        , testProperty "Integer Codec" $ integralCodec (elements [-512..512]) VarIntColumn CqlVarInt
        ]
    ]

getPutIdentity :: Val v -> Property
getPutIdentity Val{..} =
    let t = typeof value
        x = runGet (getValue version t) (runPut (putValue version value))
    in Right value === x

integralCodec :: Show a => Gen a -> ColumnType -> (a -> Value) -> Property
integralCodec g t f = forAll g $ \i ->
    let x = f i in Right x === runGet (getValue V3 t) (runPut (putValue V3 x))

toCqlFromCqlIdentity :: Value -> Property
toCqlFromCqlIdentity x@(CqlBoolean _)   = (toCql <$> (fromCql x :: Either String Bool))     === Right x
toCqlFromCqlIdentity x@(CqlInt _)       = (toCql <$> (fromCql x :: Either String Int32))    === Right x
toCqlFromCqlIdentity x@(CqlBigInt _)    = (toCql <$> (fromCql x :: Either String Int64))    === Right x
toCqlFromCqlIdentity x@(CqlSmallInt _)  = (toCql <$> (fromCql x :: Either String Int16))    === Right x
toCqlFromCqlIdentity x@(CqlTinyInt _)   = (toCql <$> (fromCql x :: Either String Int8))     === Right x
toCqlFromCqlIdentity x@(CqlFloat _)     = (toCql <$> (fromCql x :: Either String Float))    === Right x
toCqlFromCqlIdentity x@(CqlDouble _)    = (toCql <$> (fromCql x :: Either String Double))   === Right x
toCqlFromCqlIdentity x@(CqlText _)      = (toCql <$> (fromCql x :: Either String T.Text))   === Right x
toCqlFromCqlIdentity x@(CqlInet _)      = (toCql <$> (fromCql x :: Either String IP))       === Right x
toCqlFromCqlIdentity x@(CqlUuid _)      = (toCql <$> (fromCql x :: Either String UUID))     === Right x
toCqlFromCqlIdentity x@(CqlTimestamp _) = (toCql <$> (fromCql x :: Either String UTCTime))  === Right x
toCqlFromCqlIdentity x@(CqlAscii _)     = (toCql <$> (fromCql x :: Either String Ascii))    === Right x
toCqlFromCqlIdentity x@(CqlBlob _)      = (toCql <$> (fromCql x :: Either String Blob))     === Right x
toCqlFromCqlIdentity x@(CqlCounter _)   = (toCql <$> (fromCql x :: Either String Counter))  === Right x
toCqlFromCqlIdentity x@(CqlTimeUuid _)  = (toCql <$> (fromCql x :: Either String TimeUuid)) === Right x
toCqlFromCqlIdentity x@(CqlVarInt _)    = (toCql <$> (fromCql x :: Either String Integer))  === Right x
toCqlFromCqlIdentity x@(CqlDecimal _)   = (toCql <$> (fromCql x :: Either String Decimal))  === Right x
toCqlFromCqlIdentity _                  = True === True

typeof :: Value -> ColumnType
typeof (CqlBoolean _)      = BooleanColumn
typeof (CqlInt _)          = IntColumn
typeof (CqlBigInt _)       = BigIntColumn
typeof (CqlSmallInt _)     = SmallIntColumn
typeof (CqlTinyInt _)      = TinyIntColumn
typeof (CqlVarInt _)       = VarIntColumn
typeof (CqlFloat _)        = FloatColumn
typeof (CqlDecimal _)      = DecimalColumn
typeof (CqlDouble _)       = DoubleColumn
typeof (CqlText _)         = TextColumn
typeof (CqlInet _)         = InetColumn
typeof (CqlUuid _)         = UuidColumn
typeof (CqlTimestamp _)    = TimestampColumn
typeof (CqlAscii _)        = AsciiColumn
typeof (CqlBlob _)         = BlobColumn
typeof (CqlCounter _)      = CounterColumn
typeof (CqlTimeUuid _)     = TimeUuidColumn
typeof (CqlDate _)         = DateColumn
typeof (CqlTime _)         = TimeColumn
typeof (CqlMaybe Nothing)  = MaybeColumn (CustomColumn "a")
typeof (CqlMaybe (Just a)) = MaybeColumn (typeof a)
typeof (CqlList [])        = ListColumn  (CustomColumn "a")
typeof (CqlList (x:_))     = ListColumn  (typeof x)
typeof (CqlSet  [])        = SetColumn (CustomColumn "a")
typeof (CqlSet  (x:_))     = SetColumn (typeof x)
typeof (CqlMap  [])        = MapColumn (CustomColumn "a") (CustomColumn "b")
typeof (CqlMap  ((x,y):_)) = MapColumn (typeof x) (typeof y)
typeof (CqlCustom _)       = CustomColumn "a"
typeof (CqlTuple x)        = TupleColumn (map typeof x)
typeof (CqlUdt   x)        = UdtColumn "" (map (second typeof) x)

genValue :: Version -> Gen Value
genValue v =
    oneof [ simple v
          , CqlMaybe <$> oneof [Just <$> simple v, return Nothing]
          , CqlList  <$> many
          , CqlSet   <$> many
          , CqlMap   <$> (zip <$> many <*> many)
          ]
  where
    many      = simple v >>= listOf . return
    simple V2 = oneof version2
    simple V3 = oneof version3
    simple V4 = oneof version4

    version2 =
        [ CqlAscii     <$> arbitrary
        , CqlBigInt    <$> arbitrary
        , CqlBlob      <$> arbitrary
        , CqlBoolean   <$> arbitrary
        , CqlCounter   <$> arbitrary
        , CqlCustom    <$> arbitrary
        , CqlDouble    <$> arbitrary
        , CqlFloat     <$> arbitrary
        , CqlInet      <$> arbitrary
        , CqlInt       <$> arbitrary
        , CqlTimeUuid  <$> arbitrary
        , CqlTimestamp <$> arbitrary
        , CqlUuid      <$> arbitrary
        , CqlText      <$> arbitrary
        , CqlDecimal   <$> arbitrary
        , CqlVarInt    <$> arbitrary
        ]
    version3 = version2
    version4 = version3 ++
        [ CqlTime      <$> arbitrary
        , CqlDate      <$> arbitrary
        , CqlSmallInt  <$> arbitrary
        , CqlTinyInt   <$> arbitrary
        ]

data Val v = Val
    { version :: !Version
    , value   :: !Value
    } deriving Show

data V2
data V3
data V4

instance Arbitrary (Val V2) where
    arbitrary = Val V2 <$> genValue V2

instance Arbitrary (Val V3) where
    arbitrary = Val V3 <$> genValue V3

instance Arbitrary (Val V4) where
    arbitrary = Val V4 <$> genValue V4

instance Arbitrary Value where
    arbitrary = genValue V4

instance Arbitrary LB.ByteString where
    arbitrary = LB.pack <$> arbitrary

instance Arbitrary T.Text where
    arbitrary = T.pack <$> arbitrary

instance Arbitrary IP where
    arbitrary = oneof
        [ IPv4 . fromHostAddress  <$> arbitrary
        , IPv6 . fromHostAddress6 <$> arbitrary
        ]

instance Bounded UUID where
    minBound = nil
    maxBound = fromJust $ fromString "ffffffff-ffff-4fff-bfff-ffffffffffff"

instance Arbitrary UUID where
    arbitrary = arbitraryBoundedRandom

instance Arbitrary UTCTime where
    arbitrary = posixSecondsToUTCTime . fromIntegral
        <$> (arbitrary :: Gen Int64)

instance Arbitrary (DecimalRaw Integer) where
    arbitrary = Decimal <$> arbitrary <*> arbitrary
