{-# LANGUAGE
RecordWildCards,
OverloadedStrings
  #-}


module Main where


-- General
import Control.Monad
import Data.Foldable
import Data.Maybe
import Data.Functor.Identity
import Control.Monad.State
-- Lenses
import Lens.Micro
-- Lists
import Data.List (uncons)
-- Command-line options
import Options.Applicative
-- IO
import System.IO
-- Text
import Data.Char
import Text.Read (readMaybe)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Text (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
-- Containers
import qualified Data.HashMap.Strict as HM
import Data.HashMap.Strict (HashMap)
import qualified Data.Vector as V
import Data.Vector (Vector)
import qualified Data.HashSet as HS
-- Tracing
import Debug.Trace
-- Hashable
import Data.Hashable


data Command
  = Build
  | Count { cmdArtist :: Artist }

mkcmd :: String -> String -> Parser a -> Mod CommandFields a
mkcmd cmd desc parser = command cmd $ info (helper <*> parser) (progDesc desc)

cmdargs :: ParserInfo Command
cmdargs = info
  (helper <*> subparser (mconcat commands)) $
  (fullDesc <> progDesc "a thingy to recommend songs")
  where
    commands = [
      mkcmd "build" buildDesc $ pure Build,
      mkcmd "count" countDesc $ Count <$> artistP ]
    buildDesc = "build a data file out of MSD datasets"
    countDesc = "count total plays of all songs by an artist"
    artistP = T.pack <$> strArgument (metavar "ARTIST")

type UserId   = Int
type SongId   = Text
type ArtistId = Int
type Artist   = Text
type Title    = Text

data Listen = Listen {
  user   :: UserId,
  song   :: SongId,
  artist :: ArtistId,
  plays  :: Int }

data DataFile = DataFile {
  dataArtists   :: Vector Artist,
  dataArtistIds :: HashMap Artist ArtistId,
  dataSongs     :: HashMap SongId (ArtistId, Title),
  dataListens   :: [Listen] }

{- |
The file is stored as follows:

* amount of artists
* artists
* amount of songs
* (song, artist, title) tuples
* the rest is listens data

See 'buildFile' for details.
-}
readData :: TL.Text -> DataFile
readData s = flip evalState (strictLines s) $ do
  let uncons' (x:xs) = (x, xs)  
  -- Artists:
  numArtists <- tread <$> state uncons'
  artists    <- state (splitAt numArtists)
  let dataArtists   = V.fromList artists
      dataArtistIds = HM.fromList (zip artists [0..])
  -- Songs:
  numSongs <- tread <$> state uncons'
  songs    <- state (splitAt numSongs)
  let parseSong s = do
        [song, artist, title] <- return (T.splitOn "\t" s)
        return (song, (tread artist, title))
      dataSongs = HM.fromList $ mapMaybe parseSong songs
  -- Listens:
  listens <- get
  let parseListen s = do
        [user, song, artist, plays] <- return (T.splitOn "\t" s)
        return Listen{
          user   = tread user,
          song   = song,
          artist = tread artist,
          plays  = tread plays }
      dataListens = mapMaybe parseListen listens
  
  return DataFile{..}

main :: IO ()
main = do
  cmd <- execParser cmdargs
  case cmd of
    Build -> buildFile
    Count{..} -> countPlays cmdArtist

{- |
Build a data file out of unique_tracks.txt and train_triplets.txt.

Links to the dataset:

  * playcounts: <http://labrosa.ee.columbia.edu/millionsong/tasteprofile>

  * song titles: <http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/unique_tracks.txt>
-}
buildFile :: IO ()
buildFile = do
  -- First read track data from unique_tracks.txt.
  let readTracks :: TL.Text -> HashMap SongId (Artist, Title)
      readTracks = HM.fromList . mapMaybe parseTrack . strictLines
      parseTrack s = do
        [_,song, artist, title] <- return (T.splitOn "<SEP>" s)
        return (song, (artist, title))
  songsIndex <- readTracks <$> TL.readFile "unique_tracks.txt"
  
  -- Then read listens data from train_triplers.txt.
  let readListens :: TL.Text -> [(Text, SongId, Int)]
      readListens = mapMaybe parseListen . strictLines
      parseListen s = do
        [user, song, playsText] <- return (T.splitOn "\t" s)
        plays <- readMaybe (T.unpack playsText)
        (artist, title) <- HM.lookup song songsIndex
        return (user, song, plays)
  -- The listens file is big and we don't want to store it in memory, so
  -- we're going to use getListens whenever we actually need listens.
  let getListens = readListens <$> TL.readFile "train_triplets.txt"

  -- Then prepare the data.
  -- We need to have a list of artists:
  let artists = hashNub . map fst . HM.elems $ songsIndex
  -- A reverse index for artists (to be able to quickly match an artist to
  -- its index):
  let artistsIndex = HM.fromList (zip artists [0..])
  -- A reverse index for users (because numerical ids are better than guids
  -- that are used in the original file):
  usersIndex <- HM.fromList . indexConsecutive . map (^. _1) <$> getListens
  
  -- Listens:
  listens <- getListens

  -- File writing begins.
  TL.writeFile "listens.txt" . TL.unlines . map TL.fromStrict $
    -- amount of artists
    (let len = length artists
     in  trace ("artists: " ++ show len) [tshow len]) ++
    -- artists
    artists ++
    trace "done writing artists" [] ++
    -- amount of songs
    (let len = HM.size songsIndex
     in  trace ("songs: " ++ show len) [tshow len]) ++
    -- (song, artist, title) tuples
    flip map (HM.toList songsIndex) (\(song, (artist, title)) ->
      T.intercalate "\t" [
        song,
        tshow (artistsIndex HM.! artist),
        title ]) ++
    trace "done writing songs" [] ++
    -- (user, song, artist, plays) tuples
    (let len = HM.size usersIndex
     in  trace ("users: " ++ show len) []) ++
    flip map listens (\(user, song, plays) ->
      T.intercalate "\t" [
        tshow (usersIndex HM.! user),
        song,
        tshow (fst (songsIndex HM.! song)),
        tshow plays ]) ++
    trace "done writing listens" []

countPlays :: Artist -> IO ()
countPlays cmdArtist = do
  DataFile{..} <- readData <$> TL.readFile "listens.txt"
  let artistId = dataArtistIds HM.! cmdArtist
  let total = sum . map plays . filter (\x -> artist x == artistId)
            $ dataListens
  print total

-- Utils

tshow :: Show a => a -> Text
tshow = T.pack . show

tread :: Read a => Text -> a
tread = read . T.unpack

indexConsecutive :: Eq a => [a] -> [(a, Int)]
indexConsecutive = go 0
  where
    go i []     = []
    go i (x:xs) = (x, i) : go (i+1) (dropWhile (== x) xs)

hashNub :: (Eq a, Hashable a) => [a] -> [a]
hashNub = HS.toList . HS.fromList

strictLines :: TL.Text -> [Text]
strictLines = map TL.toStrict . TL.lines
