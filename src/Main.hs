{-# LANGUAGE
RecordWildCards,
OverloadedStrings
  #-}


module Main where


-- General
import Control.Monad
import Data.Foldable
import Data.Traversable
import Data.Maybe
import Control.Monad.State
import Data.Int
-- Lenses
import Lens.Micro
-- Lists
import Data.List (uncons)
-- Command-line options
import Options.Applicative
-- IO
import System.Directory
-- Text
import Text.Read (readMaybe)
import Text.Printf
import qualified Data.Text as T
import Data.Text (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
-- Containers
import qualified Data.HashMap.Strict as HM
import Data.HashMap.Strict (HashMap)
import qualified Data.HashSet as HS
-- Hashable
import Data.Hashable
-- SQL
import qualified Database.SQLite.Simple as SQL
import Database.SQLite.Simple (Only(..))


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

type UserId     = Int64
type SongTextId = Text
type SongId     = Int64
type ArtistId   = Int64
type Artist     = Text
type Title      = Text

main :: IO ()
main = do
  cmd <- execParser cmdargs
  case cmd of
    Build -> buildMusicDb
    Count{..} -> countPlays cmdArtist

-- Read track data (from unique_tracks.txt).
readTracks :: TL.Text -> HashMap SongTextId (Artist, Title)
readTracks = HM.fromList . mapMaybe parseTrack . strictLines
  where
    parseTrack s = do
      [_,song, artist, title] <- return (T.splitOn "<SEP>" s)
      return (song, (artist, title))

-- Read listens data (from train_triplets.txt).
readListens :: TL.Text -> [(Text, SongTextId, Int)]
readListens = mapMaybe parseListen . strictLines
  where
    parseListen s = do
      [user, song, playsText] <- return (T.splitOn "\t" s)
      plays <- readMaybe (T.unpack playsText)
      return (user, song, plays)

initMusicDb :: SQL.Connection -> IO ()
initMusicDb db = do
  -- A table for artists:
  SQL.execute_ db
    "CREATE TABLE artists (\
    \id INTEGER PRIMARY KEY,\
    \name TEXT NOT NULL UNIQUE)"
  SQL.execute_ db
    "CREATE INDEX index_artists_by_name ON artists (name)"
  -- A table for songs:
  SQL.execute_ db
    "CREATE TABLE songs (\
    \id INTEGER PRIMARY KEY,\
    \text_id TEXT NOT NULL,\
    \artist INTEGER NOT NULL,\
    \title TEXT NOT NULL,\
    \FOREIGN KEY(artist) REFERENCES artists(id))"
  SQL.execute_ db
    "CREATE INDEX index_songs_by_artist ON songs (artist)"
  -- A table for listens:
  SQL.execute_ db
    "CREATE TABLE listens (\
    \user INTEGER NOT NULL,\
    \song INTEGER NOT NULL,\
    \plays INTEGER NOT NULL,\
    \FOREIGN KEY(song) REFERENCES songs(id),\
    \UNIQUE (user, song))"
  SQL.execute_ db
    "CREATE INDEX index_listens_by_song ON listens (song)"

withMusicDb :: (SQL.Connection -> IO a) -> IO a
withMusicDb act = do
  db <- SQL.open "music.db"
  SQL.execute_ db "PRAGMA foreign_keys = ON"
  result <- act db
  SQL.close db
  return result

{- |
Build a data file out of unique_tracks.txt and train_triplets.txt.

Links to the dataset:

  * playcounts: <http://labrosa.ee.columbia.edu/millionsong/tasteprofile>

  * song titles: <http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/unique_tracks.txt>
-}
buildMusicDb :: IO ()
buildMusicDb = do
  -- Read track data.
  songsIndex <- readTracks <$> TL.readFile "unique_tracks.txt"
  let artists = hashNub . map fst . HM.elems $ songsIndex
  -- Read listens data.
  listens <- readListens <$> TL.readFile "train_triplets.txt"
  -- Write stuff to the database.
  exists <- doesFileExist "music.db"
  when exists $ removeFile "music.db"
  withMusicDb $ \db -> do
    initMusicDb db
    -- Trade some safety for speed.
    SQL.execute_ db "PRAGMA synchronous = OFF"
    SQL.execute_ db "PRAGMA journal_mode = MEMORY"
    -- Artists:
    SQL.withTransaction db $
      mapM_ (writeArtist db) artists
    putStrLn "done writing artists"
    -- Songs:
    songIds <- fmap HM.fromList $ SQL.withTransaction db $
      for (HM.toList songsIndex) $ \(song, (artist, title)) ->
        writeSong db song artist title
    putStrLn "done writing songs"
    -- Listens:
    let listens' = listens & each . _2 %~ (songIds HM.!)
    SQL.withTransaction db $
      foldM_ (writeListen db) ("fake user identificator", 0::Int) listens'
    putStrLn "done writing listens"
  where
    writeArtist db artist = SQL.execute db
      "INSERT INTO artists (name) VALUES (?)"
      (Only artist)
    writeSong db song artist title = do
      SQL.execute db
        "INSERT INTO songs (text_id, artist, title) \
        \VALUES (?, (SELECT id FROM artists WHERE name = ?), ?)"
        (song, artist, title)
      songId <- SQL.lastInsertRowId db
      return (song, songId)
    writeListen db (prevUser, prevUserId) (user, songId, plays) = do
      let userId | user == prevUser = prevUserId
                 | otherwise        = prevUserId + 1
      SQL.execute db
        "INSERT INTO listens (user, song, plays) \
        \VALUES (?, ?, ?)"
        (userId, songId, plays)
      return (user, userId)

countPlays :: Artist -> IO ()
countPlays cmdArtist = withMusicDb $ \db -> do
  mbArtist <- SQL.query db
    "SELECT id FROM artists WHERE name = ?"
    (Only cmdArtist)
  artist <- case mbArtist of
    [Only x] -> return (x :: ArtistId)
    []       -> error "no artists with this name"
    _        -> error "several artists with this name"
  [Only res] <- SQL.query db
    "SELECT sum(listens.plays) \
    \FROM listens \
    \INNER JOIN (SELECT * FROM songs WHERE artist=?) AS songs_by_artist \
    \ON listens.song = songs_by_artist.id"
    (Only artist)
  print (res :: Int)


-- Utils

tshow :: Show a => a -> Text
tshow = T.pack . show

tread :: Read a => Text -> a
tread = read . T.unpack

hashNub :: (Eq a, Hashable a) => [a] -> [a]
hashNub = HS.toList . HS.fromList

strictLines :: TL.Text -> [Text]
strictLines = map (T.copy . TL.toStrict) . TL.lines
