{-# LANGUAGE
RecordWildCards,
OverloadedStrings,
TupleSections,
ScopedTypeVariables
  #-}


module Main (main) where


-- General
import Control.Monad
import Data.Foldable
import Data.Traversable
import Data.Maybe
import Data.Function
import Control.Arrow
import Data.Ord
-- Numbers
import Data.Int
-- Lenses
import Lens.Micro
-- Lists
import Data.List (groupBy, sortOn)
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
import Data.HashSet (HashSet)
-- Hashing
import Data.Hashable (Hashable)
import Data.Hashabler hiding (Hashable)
-- SQL
import qualified Database.SQLite.Simple as SQL
import Database.SQLite.Simple (Only(..))
-- deepseq
import Control.DeepSeq


data Command
  = Build
  | Count {
      cmdArtist         :: Artist }
  | Best {
      cmdArtist         :: Artist,
      cmdLikedSongPlays :: Int,
      cmdLikedSongsMin  :: Int,
      cmdTopSongs       :: Int }
  | Diverse {
      cmdArtist         :: Artist,
      cmdLikedSongPlays :: Int,
      cmdLikedSongsMin  :: Int }

mkcmd :: String -> String -> Parser a -> Mod CommandFields a
mkcmd cmd desc parser = command cmd $ info (helper <*> parser) (progDesc desc)

cmdargs :: ParserInfo Command
cmdargs = info
  (helper <*> subparser (mconcat commands)) $
  (fullDesc <> progDesc "a thingy to recommend songs")
  where
    commands = [
      mkcmd "build" buildDesc $ pure Build,
      mkcmd "count" countDesc $ Count
        <$> artistP,
      mkcmd "best" bestDesc $ Best
        <$> artistP
        <*> intP "plays"       8 "how many plays should a liked song have"
        <*> intP "liked-songs" 5 "how many liked songs should a fan have"
        <*> intP "top-songs"   3 "how many liked songs of a fan to consider",
      mkcmd "diverse" diverseDesc $ Diverse
        <$> artistP
        <*> intP "plays"       8 "how many plays should a liked song have"
        <*> intP "liked-songs" 5 "how many liked songs should a fan have" ]
    buildDesc   = "build a data file out of MSD datasets"
    countDesc   = "count total plays of all songs by an artist"
    bestDesc    = "show songs by an artist which have the most fans"
    diverseDesc = "find a diverse set of good songs by an artist"
    artistP = T.pack <$> strArgument (metavar "ARTIST")
    intP name def desc = option auto $
      long name <> value def <> help desc <> metavar "INT"

type UserTextId = Text
type UserId     = Int64

type SongTextId = Text
type SongId     = Int64

type Artist     = Text
type ArtistId   = Int64

type Title      = Text

main :: IO ()
main = do
  cmd <- execParser cmdargs
  case cmd of
    Build       -> buildMusicDb
    Count{..}   -> countPlays cmdArtist
    Best{..}    -> recommendBest cmdArtist
                     cmdLikedSongPlays cmdLikedSongsMin cmdTopSongs
    Diverse{..} -> recommendDiverse cmdArtist
                     cmdLikedSongPlays cmdLikedSongsMin

{-
The Echo Nest / MSD music dataset:

  * playcounts: <http://labrosa.ee.columbia.edu/millionsong/tasteprofile>

  * song titles: <http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/unique_tracks.txt>

  * untrusted songs: <http://labrosa.ee.columbia.edu/millionsong/sites/default/files/tasteprofile/sid_mismatches.txt>
-}

-- Read track data (from unique_tracks.txt).
msdReadTracks :: TL.Text -> [((Artist, Title), SongTextId)]
msdReadTracks = mapMaybe parseTrack . strictLines
  where
    parseTrack s = do
      [_,song, artist, title] <- return (parseSsvRow "<SEP>" s)
      return ((artist, title), song)

-- Read listens data (from train_triplets.txt)
msdReadListens :: TL.Text -> [(UserId, (SongTextId, Int))]
msdReadListens = over (each._1) hashUser .
                 mapMaybe parseListen . strictLines
  where
    parseListen :: Text -> Maybe (UserTextId, (SongTextId, Int))
    parseListen s = do
      [user, song, playsText] <- return (parseSsvRow "\t" s)
      plays <- readMaybe (T.unpack playsText)
      return (user, (song, plays))
    -- Hash a user id.
    hashUser u = hashToRowId (hashFNV64 ("MSD" :: Text, u))

-- Read untrusted song ids (from sid_mismatches.txt).
msdReadUntrustedSongs :: TL.Text -> HashSet SongTextId
msdReadUntrustedSongs = HS.fromList . mapMaybe parseUntrusted . strictLines
  where
    parseUntrusted s = do
      (_:x:_) <- return (T.words s)
      guard (T.head x == '<')
      return (T.tail x)

{-
The #nowplaying dataset: http://dbis-nowplaying.uibk.ac.at/
-}

-- Read track data (from nowplaying.csv) (takes advantage of the fact that
-- input file is sorted by track).
npReadTracks :: TL.Text -> [((Artist, Title), SongTextId)]
npReadTracks = removeDuplicates . mapMaybe parseTrack . strictLines
  where
    removeDuplicates = map head . groupBy (equating snd)
    parseTrack s = do
      [_,_,_,title,artist,songId] <- return (parseSsvRow "," s)
      return ((artist, title), songId)

-- todo: use “data NpRecord” instead of parsing into a list of values

-- according to birthday paradox, the probability of collisions among several million entries should be pretty small

-- Read listens data (from nowplaying.csv).
npReadListens :: TL.Text -> [(SongTextId, [(UserId, Int)])]
npReadListens = over (each._2) countUsers . groupByTrack .
                over (each._2) hashUser .
                mapMaybe parseListen . strictLines
  where
    -- Parse a single listen from the file.
    parseListen :: Text -> Maybe (SongTextId, UserTextId)
    parseListen s = do
      [_,user,_,_,_,songId] <- return (parseSsvRow "," s)
      return (songId, user)
    -- Group listens by track (easy, since the input file is sorted by track).
    groupByTrack :: [(SongTextId, UserId)] -> [(SongTextId, [UserId])]
    groupByTrack = map (fst.head &&& map snd) . groupBy (equating fst)
    -- Count users who all listened to the same song.
    countUsers :: [UserId] -> [(UserId, Int)]
    countUsers = HM.toList . countByHashMap
    -- Hash a user id.
    hashUser u = hashToRowId (hashFNV64 ("#np" :: Text, u))

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

withNewMusicDb :: (SQL.Connection -> IO a) -> IO a
withNewMusicDb act = do
  exists <- doesFileExist "music.db"
  when exists $ removeFile "music.db"
  withMusicDb act

getArtistId :: SQL.Connection -> Artist -> IO ArtistId
getArtistId db artist = do
  mbArtistId <- SQL.query db
    "SELECT id FROM artists WHERE name=?"
    (Only artist)
  case mbArtistId of
    [Only x] -> return x
    []       -> error $ printf "no artists called “%s”" (T.unpack artist)
    _        -> error $ printf "several artists called “%s”" (T.unpack artist)

addArtist :: SQL.Connection -> Artist -> IO ()
addArtist db artist = SQL.execute db
  "INSERT INTO artists (name) VALUES (?)"
  (Only artist)

addSong :: SQL.Connection -> Artist -> Title -> IO SongId
addSong db artist title = do
  SQL.execute db
    "INSERT INTO songs (artist, title) \
    \VALUES ((SELECT id FROM artists WHERE name=?), ?)"
    (artist, title)
  SQL.lastInsertRowId db

addListens :: SQL.Connection -> [(UserId, (SongId, Int))] -> IO ()
addListens db ls = do
  -- It's possible that this combination of (user, song) already exists, and
  -- in this case we must add playcounts together.
  let query :: SQL.Query
      query = "INSERT OR REPLACE INTO listens (user, song, plays) \
              \VALUES (?, ?, \
              \        IFNULL((SELECT plays+? FROM listens \
              \                WHERE user=? AND song=?), ?))"
  SQL.withStatement db query $ \statement ->
    for_ ls $ \(user, (song, plays)) -> do
      SQL.bind statement (user, song, plays, user, song, plays)
      -- The statement we're executing is an INSERT and I don't really know
      -- what it returns – I don't care what it returns – but I used this
      -- type just in case it returns the inserted row.
      SQL.nextRow statement :: IO (Maybe (UserId, SongId, Int))
      SQL.reset statement

findDuplicateSongs ::
  [((Artist, Title), SongTextId)] ->
  HashMap (Artist, Title) [SongTextId]
findDuplicateSongs = HM.fromListWith (++) . over (each._2) (:[])
     
buildMusicDb :: IO ()
buildMusicDb = withNewMusicDb $ \db -> do
  -- Trade some safety for speed.
  SQL.execute_ db "PRAGMA synchronous = OFF"
  SQL.execute_ db "PRAGMA journal_mode = MEMORY"
  -- Initialise the database.
  initMusicDb db
  -- Read untrusted songs for the MSD dataset.
  untrusted <- msdReadUntrustedSongs <$> TL.readFile "sid_mismatches.txt"
  let isTrusted x = not (x `HS.member` untrusted)
  -- Read songs data.
  msdSongs <- filter (isTrusted . snd) .
                msdReadTracks <$> TL.readFile "unique_tracks.txt"
  npSongs <- npReadTracks <$> TL.readFile "nowplaying.csv"
  let songs :: HashMap (Artist, Title) [SongTextId]
      songs = findDuplicateSongs (msdSongs ++ npSongs)
  -- Write artists into the database.
  let artists = hashNub . map fst . HM.keys $ songs
  SQL.withTransaction db $
    mapM_ (addArtist db) artists
  putStrLn "done writing artists"
  -- Write songs into the database.
  songIds <- SQL.withTransaction db $ do
    pairs <- for (HM.toList songs) $ \((artist, title), textIds) -> do
      songId <- addSong db artist title
      return (map (,songId) textIds)
    return (HM.fromList (concat pairs))
  putStrLn "done writing songs"
  -- Read MSD listens and write them into the database.
  msdListens <- filter (isTrusted . fst . snd) .
                msdReadListens <$> TL.readFile "train_triplets.txt"
  let msdListens' = over (each._2._1) (songIds HM.!) msdListens
  SQL.withTransaction db $
    addListens db msdListens'
  putStrLn "done writing MSD listens"
  -- Read #nowplaying listens and write them into the database.
  npListens <- npReadListens <$> TL.readFile "nowplaying.csv"
  let expandNp :: (SongTextId, [(UserId, Int)]) -> [(UserId, (SongId, Int))]
      expandNp (songId, us) = do
        let song = songIds HM.! songId
        (user, plays) <- us
        return (user, (song, plays))
  let npListens' = concatMap expandNp npListens
  SQL.withTransaction db $
    addListens db npListens'
  putStrLn "done writing #nowplaying listens"

countPlays :: Artist -> IO ()
countPlays artistName = withMusicDb $ \db -> do
  artist <- getArtistId db artistName
  [Only res] <- SQL.query db
    "SELECT sum(listens.plays) \
    \FROM listens \
    \INNER JOIN (SELECT * FROM songs WHERE artist=?) AS songs_by_artist \
    \ON listens.song = songs_by_artist.id"
    (Only artist)
  print (res :: Int)

findFans :: SQL.Connection -> ArtistId -> Int -> Int -> IO [UserId]
findFans db artist likedSongPlays likedSongsMin =
  map fromOnly <$> SQL.query db
    "SELECT listens.user \
    \FROM listens \
    \INNER JOIN songs ON listens.song = songs.id \
    \WHERE listens.plays >= ? AND songs.artist=? \
    \GROUP BY listens.user \
    \HAVING count(listens.song) >= ?"
    (likedSongPlays, artist, likedSongsMin)

findSongs :: SQL.Connection -> ArtistId -> IO [SongId]
findSongs db artist =
  map fromOnly <$> SQL.query db
    "SELECT id FROM songs WHERE artist=?"
    (Only artist)

getSongTitle :: SQL.Connection -> SongId -> IO Title
getSongTitle db song =
  head . map fromOnly <$> SQL.query db
    "SELECT title FROM songs WHERE id=?"
    (Only song)

findFavorites :: SQL.Connection -> ArtistId -> UserId -> Int -> IO [SongId]
findFavorites db artist user minPlays =
  map fromOnly <$> SQL.query db
    "SELECT listens.song \
    \FROM listens \
    \INNER JOIN songs ON listens.song = songs.id \
    \WHERE listens.user=? AND songs.artist=? AND listens.plays >= ? \
    \ORDER BY listens.plays DESC"
    (user, artist, minPlays)

recommendBest :: Artist -> Int -> Int -> Int -> IO ()
recommendBest artistName likedSongPlays likedSongsMin topSongs =
  withMusicDb $ \db -> do
  artist <- getArtistId db artistName
  -- Find fans (i.e. people who like X or more songs, where “like” means
  -- “listened Y times or more”).
  artistFans <- findFans db artist likedSongPlays likedSongsMin
  -- For each fan, find nir top Z favorite songs.
  favoriteLists <- for artistFans $ \fan ->
    take topSongs <$> findFavorites db artist fan likedSongPlays
  -- Also find all songs of the artist.
  songs <- findSongs db artist
  -- For each song, say how many people like it.
  let countFans song = length (filter (song `elem`) favoriteLists)
      songs' = reverse . sortOn snd $
        [(song, fans) | song <- songs, let fans = countFans song, fans /= 0]
  for_ songs' $ \(song, fans) -> do
    title <- T.unpack <$> getSongTitle db song
    printf "%4d – %s\n" fans title

recommendDiverse :: Artist -> Int -> Int -> IO ()
recommendDiverse artistName likedSongPlays likedSongsMin  =
  withMusicDb $ \db -> do
  artist <- getArtistId db artistName
  -- Find fans and their favorite songs.
  allFans <- do
    fs <- findFans db artist likedSongPlays likedSongsMin
    for fs $ \fan ->
      (fan, ) <$> findFavorites db artist fan likedSongPlays
  -- Find all songs of the artist.
  allSongs <- findSongs db artist
  -- Now find a set of songs which pleases everyone by finding the song which
  -- pleases the most number of people, outputting it, and removing everyone
  -- who likes it from the list of people.
  let go [] _ = return ()
      go _ [] = return ()
      go songs fans = do
        let songFans :: SongId -> [UserId]
            songFans song = [fan | (fan, favs) <- fans, song `elem` favs]
        let songs' = map (id &&& songFans) songs
        let (bestSong, bestSongFans):rest = sortOn (Down . length . snd) songs'
        unless (null bestSongFans) $ do
          title <- T.unpack <$> getSongTitle db bestSong
          printf "%4d – %s\n" (length bestSongFans) title
          let fans' = filter (\(fan,_) -> fan `notElem` bestSongFans) fans
          go (map fst rest) fans'
  go allSongs allFans

-- Utils

hashNub :: (Eq a, Hashable a) => [a] -> [a]
hashNub = HS.toList . HS.fromList

strictLines :: TL.Text -> [Text]
strictLines = map (T.copy . TL.toStrict) . TL.lines

-- | Parse a row of values separated by some separator. Values can be quoted
-- or unquoted; if quoted, standard CSV rules apply (separators can appear in
-- quotes and then they don't count as separators, «""» counts as «"»).
parseSsvRow :: Text -> Text -> [Text]
parseSsvRow sep s = force $ map T.copy $ go (T.splitOn sep s)
  where
    go [] = []
    go (x:xs) = case T.uncons x of
      Just ('"', rest) -> goQuoted [] (rest:xs)
      _other           -> x : go xs
    reconstruct = T.intercalate sep . reverse
    goQuoted xs [] = [reconstruct xs]
    goQuoted xs (p:ps)
      | even trailingQuotes = goQuoted (p':xs) ps
      | otherwise           = reconstruct (T.init p' : xs) : go ps
      where
        trailingQuotes = T.length p - T.length (T.dropWhileEnd (== '"') p)
        p' = T.replace "\"\"" "\"" p

equating :: Eq b => (a -> b) -> (a -> a -> Bool)
equating f = (==) `on` f

countByHashMap :: (Eq a, Hashable a) => [a] -> HashMap a Int
countByHashMap = HM.fromListWith (+) . map (,1)

hashToRowId :: Hash64 -> Int64
hashToRowId = fromIntegral . hashWord64
