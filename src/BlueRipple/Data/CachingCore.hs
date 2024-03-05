{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
--{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Data.CachingCore
  (
    module BlueRipple.Data.CachingCore
  )
where

import qualified Control.Foldl as FL
import qualified Control.Monad.Primitive as Prim
import qualified Data.ByteString as BS
import qualified Data.Sequence as Seq
import qualified Data.Text as T
import qualified Data.Vector as Vector
import qualified Data.Vinyl as V
import qualified Flat
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.Serialize as FS
import qualified Knit.Effect.Serialize as KS
import qualified Knit.Report as K
import qualified System.Random.MWC as MWC

insureFinalSlash :: Text -> Maybe Text
insureFinalSlash t = f <$> T.unsnoc t
  where
    f (_, l) = if l == '/' then t else T.snoc t '/'

insureFinalSlashE :: Either Text Text -> Maybe (Either Text Text)
insureFinalSlashE e = case e of
  Left t -> Left <$> insureFinalSlash t
  Right t -> Right <$> insureFinalSlash t


mapDirE :: (Text -> Text) -> Either Text Text -> Either Text Text
mapDirE f (Left t) = Left $ f t
mapDirE f (Right t) = Right $ f t

cacheFromDirE :: (K.KnitEffects r, CacheEffects r)
              => Either Text Text -> Text -> K.Sem r Text
cacheFromDirE dirE n = do
  dirE' <- K.knitMaybe "cacheDirFromDirE: empty directory given in dirE argument!" $ insureFinalSlashE dirE
  case dirE' of
    Left cd -> clearIfPresentD' (cd <> n)
    Right cd -> pure (cd <> n)

clearIfPresentD' :: (K.KnitEffects r, CacheEffects r) => T.Text -> K.Sem r Text
clearIfPresentD' k = do
  K.logLE K.Warning $ "Clearing cached item with key=" <> show k
  K.clearIfPresent @T.Text @CacheData k
  pure k

clearIf' :: (K.KnitEffects r, CacheEffects r) => Bool -> Text -> K.Sem r Text
clearIf' flag k = if flag then clearIfPresentD' k else pure k

clearIfPresentD :: (K.KnitEffects r, CacheEffects r) => T.Text -> K.Sem r ()
clearIfPresentD k = void $ clearIfPresentD' k

retrieveOrMakeD ::
  ( K.KnitEffects r,
    CacheEffects r,
    SerializerC b
  ) =>
  T.Text ->
  K.ActionWithCacheTime r a ->
  (a -> K.Sem r b) ->
  K.Sem r (K.ActionWithCacheTime r b)
retrieveOrMakeD = K.retrieveOrMake @SerializerC @CacheData @T.Text

retrieveOrMakeFrame ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs,
    V.RMap rs,
    FI.RecVec rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMakeFrame key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeFrame (key=" <> key <> ")") $
    K.retrieveOrMakeTransformed @SerializerC @CacheData fromFrame toFrame key cachedDeps action

retrieveOrMakeFrameAnd ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs,
    V.RMap rs,
    FI.RecVec rs,
    SerializerC c
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs, c)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs, c)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMakeFrameAnd key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeFrameAnd (key=" <> key <> ")") $ do
    let toFirst = first fromFrame
        fromFirst = first toFrame
    K.retrieveOrMakeTransformed  @SerializerC @CacheData toFirst fromFirst key cachedDeps action

retrieveOrMake2Frames ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs1,
    V.RMap rs1,
    FI.RecVec rs1,
    RecSerializerC rs2,
    V.RMap rs2,
    FI.RecVec rs2
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs1, F.FrameRec rs2)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs1, F.FrameRec rs2)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMake2Frames key cachedDeps action =
  let from (f1, f2) = (toFrame f1, toFrame f2)
      to (s1, s2) = (fromFrame s1, fromFrame s2)
   in K.wrapPrefix ("BlueRipple.retrieveOrMake2Frames (key=" <> key <> ")") $
        K.retrieveOrMakeTransformed  @SerializerC @CacheData to from key cachedDeps action

retrieveOrMake3Frames ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs1,
    V.RMap rs1,
    FI.RecVec rs1,
    RecSerializerC rs2,
    V.RMap rs2,
    FI.RecVec rs2,
    RecSerializerC rs3,
    V.RMap rs3,
    FI.RecVec rs3
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs1, F.FrameRec rs2, F.FrameRec rs3)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs1, F.FrameRec rs2, F.FrameRec rs3)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMake3Frames key cachedDeps action =
  let from (f1, f2, f3) = (toFrame f1, toFrame f2, toFrame f3)
      to (s1, s2, s3) = (fromFrame s1, fromFrame s2, fromFrame s3)
   in K.wrapPrefix ("BlueRipple.retrieveOrMake3Frames (key=" <> key <> ")") $
        K.retrieveOrMakeTransformed  @SerializerC @CacheData to from key cachedDeps action

retrieveOrMakeRecList ::
  ( K.KnitEffects r
  , CacheEffects r
  , RecSerializerC rs
  , V.RMap rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r [F.Record rs]) ->
  K.Sem r (K.ActionWithCacheTime r [F.Record rs])
retrieveOrMakeRecList key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeRecList (key=" <> key <> ")") $
    K.retrieveOrMakeTransformed  @SerializerC @CacheData (fmap FS.toS) (fmap FS.fromS) key cachedDeps action


type SerializerC = Flat.Flat
type RecSerializerC rs = FS.RecFlat rs

type CacheData = BS.ByteString
type CacheEffects r = K.CacheEffects SerializerC CacheData T.Text r

fromFrame :: F.Frame a -> FS.SFrame a
fromFrame = FS.SFrame
{-# INLINABLE fromFrame #-}

toFrame :: FS.SFrame a -> F.Frame a
toFrame = FS.unSFrame
{-# INLINABLE toFrame #-}

flatSerializeDict :: KS.SerializeDict Flat.Flat BS.ByteString
flatSerializeDict =
  KS.SerializeDict
  Flat.flat
  (first (KS.SerializationError . show) . Flat.unflat)
  id
  id
  (fromIntegral . BS.length)
{-# INLINEABLE flatSerializeDict #-}


sampleFrame :: (FI.RecVec rs, Prim.PrimMonad m) => Word32 -> Int -> F.FrameRec rs -> m (F.FrameRec rs)
sampleFrame seed n rows = do
  gen <- MWC.initialize (Vector.singleton seed)
  F.toFrame <$> sample (FL.fold FL.list rows) n gen

sample :: Prim.PrimMonad m => [a] -> Int -> MWC.Gen (Prim.PrimState m) -> m [a]
sample ys size = go 0 (l - 1) (Seq.fromList ys) where
    l = length ys
    go !n !i xs g | n >= size = return $! (toList . Seq.drop (l - size)) xs
                  | otherwise = do
                      j <- MWC.uniformR (0, i) g
                      let toI  = xs `Seq.index` j
                          toJ  = xs `Seq.index` i
                          next = (Seq.update i toI . Seq.update j toJ) xs
                      go (n + 1) (i - 1) next g
{-# INLINEABLE sample #-}
