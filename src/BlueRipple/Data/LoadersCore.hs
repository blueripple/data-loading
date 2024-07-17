{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Data.LoadersCore
  (
    module BlueRipple.Data.LoadersCore
  )
where

import qualified BlueRipple.Data.CachingCore   as BRC
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Polysemy as Polysemy
import qualified Polysemy.Error as Polysemy

import qualified Knit.Report                   as K
import qualified Knit.Utilities.Streamly       as K

import qualified Text.Pandoc                   as Pandoc

import qualified Control.Monad.Catch.Pure      as Exceptions
import qualified Control.Monad.State           as ST
import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import qualified Data.Map                      as Map
--import           Data.Serialize.Text            ( )
import qualified Data.Text                     as T

import qualified Data.Vinyl                    as V

import qualified Streamly.Data.Stream as Streamly
import qualified Streamly.Data.Stream.Prelude as SP
import qualified Streamly.Internal.Data.Stream as Streamly

import qualified Streamly.Data.Fold as Streamly.Fold
import qualified Streamly.Internal.Data.Fold as Streamly.Fold

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Frames                        as F
import qualified Frames.Streamly.InCore        as FS
import qualified Frames.Streamly.CSV        as FS
import qualified Frames.Streamly.Streaming.Class as FS (StreamFunctions(..), StreamFunctionsIO(..))
import Frames.Streamly.Streaming.Streamly (StreamlyStream(..))
import qualified Frames.MaybeUtils             as FM

import qualified System.Directory as System
import qualified System.Clock
import GHC.TypeLits (Symbol)

type Stream = Streamly.Stream
type MonadAsync m = SP.MonadAsync m

logFrame ::
  (BRK.KnitEffects r, Foldable f, Show (F.Record rs)) =>
  f (F.Record rs) ->
  K.Sem r ()
logFrame = logFrame' K.Info
{-# INLINEABLE logFrame #-}

logFrame' ::
  (BRK.KnitEffects r, Foldable f, Show (F.Record rs))
  => K.LogSeverity
  -> f (F.Record rs)
  ->K.Sem r ()
logFrame' ll fr = do
  let (nRows, asList) = FL.fold ((,) <$> FL.length <*> FL.list) fr
  K.logLE ll $ show nRows <> "rows:\n" <> T.intercalate "\n" (fmap show asList)
{-# INLINEABLE logFrame' #-}

logCachedFrame ::
  (BRK.KnitEffects r, Foldable f, Show (F.Record rs)) =>
  K.ActionWithCacheTime r (f (F.Record rs)) ->
  K.Sem r ()
logCachedFrame fr_C =  K.ignoreCacheTime fr_C >>= logFrame
--  fr <- K.ignoreCacheTime fr_C
--  K.logLE K.Info $ "\n" <> (T.intercalate "\n" . fmap show $ FL.fold FL.list fr)


sMap :: Monad m => (a -> b) -> Stream m a  -> Stream m b
sMap = fmap
{-# INLINE sMap #-}

data DataPath = LocalData T.Text

getPath :: DataPath -> IO FilePath
getPath dataPath = case dataPath of
--  DataSets fp -> liftIO $ BR.dataPath (toString fp)
  LocalData fp -> return (toString fp)

-- if the file is local, use modTime
-- if the file is from the data-set library, assume the cached version is good since the modtime
-- may change any time that library is re-installed.
dataPathWithCacheTime :: Applicative m => DataPath -> IO (K.WithCacheTime m DataPath)
dataPathWithCacheTime dp@(LocalData _) = do
  modTime <- getPath dp >>= System.getModificationTime
  return $ K.withCacheTime (Just modTime) (pure dp)
--dataPathWithCacheTime dp@(DataSets _) = return $ K.withCacheTime Nothing (pure dp)

recStreamLoader
  :: forall qs rs m
   . ( --V.RMap rs
     V.RMap qs
     , FS.StrictReadRec qs
     , MonadAsync m
     , FL.PrimMonad m
     , Exceptions.MonadCatch m
     )
  => DataPath
  -> Maybe FS.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Stream m (F.Record rs)
recStreamLoader dataPath parserOptionsM mFilter fixRow = Streamly.concatEffect $ do
  let csvParserOptions =
        FS.defaultParser --{ F.quotingMode = F.RFC4180Quoting '"' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
      filterF !r = fromMaybe (const True) mFilter r
      strictFix !r = fixRow r
      notifyEvery = 250000
  path <- liftIO $ getPath dataPath
  pure
    $ sMap strictFix
    $! Streamly.tapOffsetEvery
    notifyEvery
    notifyEvery
    (runningCountF
      ("Read (# rows, from \"" <> toText path <> "\")")
      (\n -> " "
             <> ("from \""
                  <> toText path
                  <> "\": "
                  <> (show $ notifyEvery * n))
      )
      ("loading \"" <> toText path <> "\" from disk finished.")
    )
    $! loadToRecStream @qs parserOptions path filterF

-- file/rowGen has qs
-- Filter qs
-- transform to rs
cachedFrameLoader
  :: forall qs rs r
   . ( V.RMap rs
     , V.RMap qs
     , FS.StrictReadRec qs
     , FS.RecVec rs
     , BRC.RecSerializerC rs
     , BRK.KnitEffects r
     , BRC.CacheEffects r
     )
  => DataPath
  -> Maybe FS.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs))
cachedFrameLoader filePath parserOptionsM mFilter fixRow cachePathM key = do
  let cacheKey      = fromMaybe "data/" cachePathM <> key
  cachedDataPath :: K.ActionWithCacheTime r DataPath <- liftIO $ dataPathWithCacheTime filePath
  K.logLE (K.Debug 3) $ "loading or retrieving and saving data at key=" <> cacheKey
  BRC.retrieveOrMakeFrame cacheKey cachedDataPath $ \dataPath -> do
    let recStream = recStreamLoader @qs @rs dataPath parserOptionsM mFilter fixRow
    f <- K.streamlyToKnit $ FS.inCoreAoS @_ @rs @StreamlyS $ StreamlyStream recStream
    path <- liftIO $ getPath filePath
    K.logLE K.Info $ "(Re)loaded data from " <> toText path <> ". Got " <> show (F.frameLength f) <> " rows."
    pure f


type StreamlyS = StreamlyStream Stream

-- file has qs
-- Filter qs
-- transform to rs
-- This one uses "FrameLoader" directly since there's an efficient disk to Frame
-- routine available.
frameLoader
  :: forall qs rs r
  . (BRK.KnitEffects r
    , FS.StrictReadRec qs
    , FS.RecVec qs
    , V.RMap qs
    )
  => DataPath
  -> Maybe FS.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> K.Sem r (F.FrameRec rs)
frameLoader filePath mParserOptions mFilter fixRow = do
  let csvParserOptions =
        FS.defaultParser --
--        { FS.quotingMode = F.RFC4180Quoting ' ' }
      parserOptions = fromMaybe csvParserOptions mParserOptions
      filterF !r = fromMaybe (const True) mFilter r
      strictFix !r = fixRow r
  path <- liftIO $ getPath filePath
  K.logLE (K.Debug 3) ("Attempting to load data from " <> toText path <> " into a frame.")
  fmap strictFix <$> loadToFrame parserOptions path filterF

frameLoader' :: forall qs rs r
                . (BRK.KnitEffects r
                  , FS.StrictReadRec qs
                  , FS.RecVec rs
                  , V.RMap qs
                  , V.RFoldMap qs
                  , V.RecordToList qs
                  , V.RPureConstrained V.KnownField qs
                  , V.RecApplicative qs
                  , V.RApply qs
                  , qs F.⊆ qs
                  , V.ReifyConstraint Show (Maybe F.:. F.ElField) qs
                  , V.ReifyConstraint Show F.ElField qs
                  )
             => DataPath
             -> Maybe FS.ParserOptions
             -> (F.Record qs -> F.Record rs)
             -> K.Sem r (F.FrameRec rs)
frameLoader' filePath mParserOptions fixRow =
  maybeFrameLoader @qs @qs @qs filePath mParserOptions Nothing id fixRow


-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
maybeFrameLoader
  :: forall (fs :: [(Symbol, Type)]) qs qs' rs r
   . ( --V.RMap rs
     V.RMap fs
     , FS.StrictReadRec fs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , V.RFoldMap qs'
     , V.RPureConstrained V.KnownField qs'
     , V.RecApplicative qs'
     , V.RApply qs'
     , qs F.⊆ fs
     , FS.RecVec rs
     , BRK.KnitEffects r
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     , V.ReifyConstraint Show (Maybe F.:. F.ElField) qs'
     , V.RMap qs'
     , V.RecordToList qs'
     )
  => DataPath
  -> Maybe FS.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs')
  -> (F.Record qs' -> F.Record rs)
  -> K.Sem r (F.FrameRec rs)
maybeFrameLoader  dataPath parserOptionsM mFilterMaybes fixMaybes transformRow
  = K.streamlyToKnit $ FS.inCoreAoS $ StreamlyStream $ maybeRecStreamLoader @fs @qs @qs' @rs dataPath parserOptionsM mFilterMaybes fixMaybes transformRow



-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
maybeRecStreamLoader
  :: forall fs qs qs' rs
   . ( V.RMap fs
     , FS.StrictReadRec fs
     , V.RFoldMap qs
     , V.RMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , V.RFoldMap qs'
     , V.RPureConstrained V.KnownField qs'
     , V.RecApplicative qs'
     , V.RApply qs'
     , qs F.⊆ fs
     , Show (F.Record qs)
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     , V.ReifyConstraint Show (Maybe F.:. F.ElField) qs'
     , V.RMap qs'
     , V.RecordToList qs'
     )
  => DataPath
  -> Maybe FS.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs')
  -> (F.Record qs' -> F.Record rs)
  -> Stream K.StreamlyM (F.Record rs)
maybeRecStreamLoader dataPath mParserOptions mFilterMaybes fixMaybes transformRow = Streamly.concatEffect $ do
  let csvParserOptions = FS.defaultParser
      parserOptions = fromMaybe csvParserOptions mParserOptions
      filterMaybes !r = fromMaybe (const True) mFilterMaybes r
      strictTransform r = transformRow r
  path <- liftIO $ getPath dataPath
  pure
    $ Streamly.map strictTransform
    $ processMaybeRecStream fixMaybes (const True)
    $ loadToMaybeRecStream @fs parserOptions path filterMaybes

-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
cachedMaybeFrameLoader
  :: forall fs qs qs' rs r
   . ( BRK.KnitEffects r
     , BRC.CacheEffects r
     , V.RMap rs
     , V.RMap fs
     , FS.StrictReadRec fs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , V.RFoldMap qs'
     , V.RPureConstrained V.KnownField qs'
     , V.RecApplicative qs'
     , V.RApply qs'
     , qs F.⊆ fs
     , FS.RecVec rs
     , BRC.RecSerializerC rs
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     , V.ReifyConstraint Show (Maybe F.:. F.ElField) qs'
     , V.RMap qs'
     , V.RecordToList qs'
     )
  => DataPath
  -> Maybe FS.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs')
  -> (F.Record qs' -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs))
cachedMaybeFrameLoader dataPath mParserOptions mFilterMaybes fixMaybes transformRow cachePathM key = do
  let cachePath = fromMaybe "data" cachePathM
      cacheKey = cachePath <> "/" <> key
  filePath <- liftIO $ getPath dataPath
  cachedDataPath :: K.ActionWithCacheTime r DataPath <- liftIO $ dataPathWithCacheTime dataPath
  K.logLE (K.Debug 3) $ "loading " <> toText filePath <> " or retrieving and saving data at key=" <> cacheKey
  BRC.retrieveOrMakeFrame cacheKey cachedDataPath $ \dataPath' -> do
    f <- maybeFrameLoader @fs dataPath' mParserOptions mFilterMaybes fixMaybes transformRow
    K.logLE K.Info $ "(Re)loaded data from " <> toText filePath <> ". Got " <> show (F.frameLength f) <> " rows."
    pure f

rmapM :: Monad m => (a -> m b) -> Streamly.Fold.Fold m x a -> Streamly.Fold.Fold m x b
rmapM = Streamly.Fold.rmapM
{-# INLINE rmapM #-}

toPipes :: Monad m => Streamly.Stream m a -> P.Producer a m ()
toPipes = P.unfoldr unconsS
    where
    -- Adapt S.uncons to return an Either instead of Maybe
    unconsS s = Streamly.uncons s >>= maybe (return $ Left ()) (return . Right)
{-# INLINEABLE toPipes #-}

loadToRecStream
  :: forall rs m
  . ( Monad m
    , FL.PrimMonad m
    , MonadAsync m
    , Exceptions.MonadCatch m
    , V.RMap rs
    , FS.StrictReadRec rs
    )
  => FS.ParserOptions
  -> FilePath
  -> (F.Record rs -> Bool)
  -> Streamly.Stream m (F.Record rs)
loadToRecStream po fp filterF = Streamly.filter filterF
  $ stream $ FS.readTableOpt @rs @(StreamlyStream Streamly.Stream) po fp
{-# INLINEABLE loadToRecStream #-}

-- load with cols qs
-- filter with rs
-- return rs
loadToMaybeRecStream
  :: forall qs rs m
  . ( Monad m
    , FL.PrimMonad m
    , MonadAsync m
    , Exceptions.MonadCatch m
    , FS.StrictReadRec qs
    , V.RMap rs
    , Show (F.Record rs)
    , V.RMap qs
    , V.RecordToList rs
    , (V.ReifyConstraint Show (Maybe F.:. F.ElField) rs)
    , rs F.⊆ qs
    )
  => FS.ParserOptions
  -> FilePath
  -> (F.Rec (Maybe F.:. F.ElField) rs -> Bool)
  -> Streamly.Stream m (F.Rec (Maybe F.:. F.ElField) rs)
loadToMaybeRecStream po fp filterF =
  Streamly.filter filterF
  $ fmap F.rcast
  $ stream
  $ FS.readTableMaybeOpt @qs @(StreamlyStream Streamly.Stream) po fp

logLengthF :: T.Text -> Streamly.Fold.Fold K.StreamlyM a ()
logLengthF t = rmapM (\n -> K.logStreamly K.Diagnostic $ t <> " " <> (T.pack $ show n)) Streamly.Fold.length

loadToRecList
  :: forall rs r
  . ( BRK.KnitEffects r
    , FS.StrictReadRec rs
    , V.RMap rs
    )
  => FS.ParserOptions
  -> FilePath
  -> (F.Record rs -> Bool)
  -> K.Sem r [F.Record rs]
loadToRecList po fp filterF = K.streamlyToKnit $ Streamly.toList $ loadToRecStream po fp filterF
{-# INLINEABLE loadToRecList #-}

loadToFrame
  :: forall rs r
   . ( BRK.KnitEffects r
     , FS.StrictReadRec rs
     , FS.RecVec rs
     , V.RMap rs
     )
  => FS.ParserOptions
  -> FilePath
  -> (F.Record rs -> Bool)
  -> K.Sem r (F.FrameRec rs)
loadToFrame po fp filterF = do
  frame <- K.streamlyToKnit
           $ FS.inCoreAoS
           $ StreamlyStream
           $ Streamly.filter filterF
           $ stream
           $ FS.readTableOpt @rs @StreamlyS po fp
  let reportRows :: Foldable f => f x -> FilePath -> K.Sem r ()
      reportRows f fn =
        K.logLE K.Diagnostic
          $  T.pack (show $ FL.fold FL.length f)
          <> " rows in "
          <> T.pack fn
  reportRows frame fp
  return frame
{-# INLINEABLE loadToFrame #-}

processMaybeRecStream
  :: forall rs rs'
     .(V.RFoldMap rs
     , V.RPureConstrained V.KnownField rs
     , V.RecApplicative rs
     , V.RApply rs
     , V.RFoldMap rs'
     , V.RPureConstrained V.KnownField rs'
     , V.RecApplicative rs'
     , V.RApply rs'
     , Show (F.Rec (Maybe F.:. F.ElField) rs)
     , V.ReifyConstraint Show (Maybe F.:. F.ElField) rs'
     , V.RMap rs'
     , V.RecordToList rs'
     , V.ReifyConstraint Show (Maybe F.:. F.ElField) rs'
     , V.RMap rs'
     , V.RecordToList rs'
     , V.ReifyConstraint Show (Maybe F.:. F.ElField) rs
     , V.RMap rs
     , V.RecordToList rs
     )
  => (F.Rec (Maybe F.:. F.ElField) rs -> (F.Rec (Maybe F.:. F.ElField) rs')) -- fix any Nothings you need to/can
  -> (F.Record rs' -> Bool) -- filter after removing Nothings
  -> Stream K.StreamlyM (F.Rec (Maybe F.:. F.ElField) rs)
  -> Stream K.StreamlyM (F.Record rs')
processMaybeRecStream fixMissing filterRows maybeRecS = do
  let addMissing m l = Map.insertWith (+) l 1 m
      addMissings m = FL.fold (FL.Fold addMissing m id)
      whatsMissingF ::  (V.RFoldMap qs
                        , V.RPureConstrained V.KnownField qs
                        , V.RecApplicative qs, V.RApply qs
                        , Show (F.Rec (Maybe F.:. F.ElField) qs)
                        , V.ReifyConstraint Show (Maybe F.:. F.ElField) qs
                        , V.RecordToList qs
                        , V.RMap qs
                        )
                    => Streamly.Fold.Fold K.StreamlyM (F.Rec  (Maybe F.:. F.ElField) qs) (Maybe Text, Map Text Int)
      whatsMissingF = Streamly.Fold.foldl' (\(rM, m) a -> (maybe (Just $ show a) Just rM, addMissings m (FM.whatsMissingRow a))) (Nothing, Map.empty)
      logMissingF ::  (V.RFoldMap qs, V.RPureConstrained V.KnownField qs, V.RecApplicative qs, V.RApply qs
                      , V.ReifyConstraint Show (Maybe F.:. F.ElField) qs
                      , V.RecordToList qs
                      , V.RMap qs
                      )
                  =>T.Text -> Streamly.Fold.Fold K.StreamlyM (F.Rec (Maybe F.:. F.ElField) qs) ()
      logMissingF t = rmapM (\(mRow, x) -> K.logStreamly K.Diagnostic $ t <> (T.pack $ show x)
                                           <> maybe "" ("\nExample: " <>) mRow) whatsMissingF
  Streamly.filter filterRows
    $ Streamly.tap (logLengthF "Length after fixing and dropping: ")
    $ Streamly.mapMaybe F.recMaybe
    $ Streamly.tap (logMissingF "missing after fixing: ")
    $ fmap fixMissing
    $ Streamly.tap (logMissingF "missing before fixing: ")
    $ maybeRecS
{-# INLINEABLE processMaybeRecStream #-}

someExceptionAsPandocError :: forall r a. (Polysemy.Member (Polysemy.Error K.PandocError) r)
                           => Polysemy.Sem (Polysemy.Error SomeException ': r) a -> Polysemy.Sem r a
someExceptionAsPandocError = Polysemy.mapError (\(e :: SomeException) -> Pandoc.PandocSomeError $ T.pack $ displayException e)
{-# INLINEABLE someExceptionAsPandocError #-}

-- This goes through maybeRecs so we can see if we're dropping rows.  Slightly slower and more
-- memory intensive (I assume)
-- but since we will cache anything big post-processed, that seems like a good trade often.
loadToRecListChecked
  :: forall rs effs
   . ( MonadIO (K.Sem effs)
     , K.LogWithPrefixesLE effs
     , FS.StrictReadRec rs
     , FS.RecVec rs
     , V.RFoldMap rs
     , V.RMap rs
     , V.RPureConstrained V.KnownField rs
     , V.RecApplicative rs
     , V.RApply rs
     , rs F.⊆ rs
     )
  => FS.ParserOptions
  -> FilePath
  -> (F.Record rs -> Bool)
  -> K.Sem effs [F.Record rs]
loadToRecListChecked po fp filterF = loadToMaybeRecs @rs @rs po (const True) fp >>= processMaybeRecs id filterF
--  traverse (maybeRecsToFrame id filterF) $ loadToMaybeRecs po (const True) fp

loadToMaybeRecs
  :: forall rs rs' effs
   . ( MonadIO (K.Sem effs)
     , K.LogWithPrefixesLE effs
     , FS.StrictReadRec rs'
     , FS.RecVec rs'
     , V.RMap rs'
     , rs F.⊆ rs'
     )
  => FS.ParserOptions
  -> (F.Rec (Maybe F.:. F.ElField) rs -> Bool)
  -> FilePath
  -> K.Sem effs [F.Rec (Maybe F.:. F.ElField) rs]
loadToMaybeRecs po filterF fp = do
  let producerM = FS.readTableMaybeOpt @rs' @StreamlyS @IO po fp --P.>-> P.filter filterF
  listM :: [F.Rec (Maybe F.:. F.ElField) rs] <-
    liftIO
    $ FS.runSafe @StreamlyS @IO
    $ FS.sToList
    $ FS.sMapMaybe (\r -> if filterF r then Just r else Nothing)
    $ FS.sMap F.rcast
    $ producerM
--    $
--    P.>-> P.map F.rcast
--    P.>-> P.filter filterF
  let reportRows :: Foldable f => f x -> FilePath -> K.Sem effs ()
      reportRows f fn =
        K.logLE K.Diagnostic
          $  T.pack (show $ FL.fold FL.length f)
          <> " rows in "
          <> T.pack fn
  reportRows listM fp
  return listM

processMaybeRecs
  :: ( K.LogWithPrefixesLE effs
     , V.RFoldMap rs
     , V.RPureConstrained V.KnownField rs
     , V.RecApplicative rs
     , V.RApply rs
--     , Show (F.Rec (Maybe F.:. F.ElField) rs)
     )
  => (F.Rec (Maybe F.:. F.ElField) rs -> (F.Rec (Maybe F.:. F.ElField) rs)) -- fix any Nothings you need to/can
  -> (F.Record rs -> Bool) -- filter after removing Nothings
  -> [F.Rec (Maybe F.:. F.ElField) rs]
  -> K.Sem effs [F.Record rs]
processMaybeRecs fixMissing filterRows maybeRecs =
  K.wrapPrefix "maybeRecsToFrame" $ do
    let missingPre = FM.whatsMissing maybeRecs
        fixed       = fmap fixMissing maybeRecs
        missingPost = FM.whatsMissing fixed
        droppedMissing = catMaybes $ fmap F.recMaybe fixed
        filtered = L.filter filterRows droppedMissing
    K.logLE K.Diagnostic "Input rows:"
    K.logLE K.Diagnostic (T.pack $ show $ length maybeRecs)
    K.logLE K.Diagnostic "Missing Data before fixing:"
    K.logLE K.Diagnostic (T.pack $ show missingPre)
    K.logLE K.Diagnostic "Missing Data after fixing:"
    K.logLE K.Diagnostic (T.pack $ show missingPost)
    K.logLE K.Diagnostic "Rows after fixing and dropping missing:"
    K.logLE K.Diagnostic (T.pack $ show $ length droppedMissing)
    K.logLE K.Diagnostic "Rows after filtering: "
    K.logLE K.Diagnostic (T.pack $ show $ length filtered)
    return filtered

-- tracing fold
#if MIN_VERSION_streamly_core(0,2,0)
runningCountF :: ST.MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done done where
#else
runningCountF :: ST.MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
#endif
  start = ST.liftIO (putText startMsg) >> return (Streamly.Fold.Partial 0)
  step !n _ = ST.liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    putTextLn $ countMsg n
    pure $ Streamly.Fold.Partial (n + 1)
  done _ = ST.liftIO $ putTextLn endMsg
