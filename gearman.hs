{-# OPTIONS -fglasgow-exts #-}
{-# OPTIONS -fno-monomorphism-restriction #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE OverlappingInstances #-}

import Control.Applicative
import Control.Arrow
import Control.Concurrent
import Control.Monad
import Control.Monad.State
import Data.Bits
import qualified Data.ByteString.Char8 as BS
import Data.Char
import Data.List
import qualified Data.Map as M
import Data.Word
import IO
import qualified Network as N

data Accessor a b = Accessor { inspect :: a -> b, mutate :: (b -> b) -> a -> a }

-- inspect a = fst (acc a id)
-- mutate a f = snd (acc a f)
acc :: Accessor a b -> (b -> b) -> a -> (b, a)
acc accessor f value = (inspect accessor &&& id) (mutate accessor f value)

accId = Accessor id ($)

accFirst = Accessor fst first
accSecond = Accessor snd second

accListElt index = Accessor (!! index) (\f -> splitAt index >>> second (splitAt 1 >>> first (liftM f) >>> uncurry (++)) >>> uncurry (++))

accMapElt key = Accessor (M.! key) (\f -> M.update (return . f) key)

class Wrapped a b where
    wrap :: b -> a
    unwrap :: a -> b

data Pair a b = Pair { pair :: (a, b) }

instance Wrapped (Pair a b) (a, b) where
    wrap = Pair
    unwrap = pair

accWrapped (Accessor f g) = Accessor (f . unwrap) (\h -> (wrap . g h . unwrap))

accPairFst = accWrapped accFirst
accPairSnd = accWrapped accSecond

(Accessor f g) .\ (Accessor f' g') = Accessor (f' . f) (g . g')
(Accessor f g) .* (Accessor f' g') = Accessor (f &&& f') (\h value -> let (x', y') = (f &&& f' >>> h) value in (g (const x') . g' (const y')) value)

replace = (. const) . mutate

(.>) = flip inspect
value .< accessor = (\f -> mutate accessor f value)
value .<< accessor = (\x -> replace accessor x value)

stateInspect accessor = get >>= return . inspect accessor
stateMutate accessor f = get >>= put . mutate accessor f
stateReplace accessor x = get >>= put . replace accessor x

(~.>) = stateInspect
(~.<) = stateMutate
(~.<<) = stateReplace

stateEmbedMutation accessor embeddedMutation = do
    curState <- (~.>) accessor
    (returnValue, newState) <- runStateT embeddedMutation curState
    accessor ~.<< newState
    return returnValue

(~.~) = stateEmbedMutation

listAccessors accessor = do
    listElts <- (~.>) accessor
    return (map ((accessor .\) . accListElt . fst) (zip [0 ..] listElts))

mapAccessors accessor = do
    mapElts <- (~.>) accessor
    return (map ((accessor .\) . accMapElt . fst) (M.toList mapElts))

instance Show N.PortID where
    show (N.Service str) = "{<service:{" ++ str ++ "}>}"
    show (N.PortNumber num) = "{<port:{" ++ (show num) ++ "}>}"
    show (N.UnixSocket str) = "{<socket:{" ++ str ++ "}>}"

charBits = bitSize (undefined :: Word8)

explode :: Char -> BS.ByteString -> [BS.ByteString]
explode = unfoldr . (f' .) . (BS.span . (/=))
    where
        f' (x, y) = if BS.null x && BS.null y
            then Nothing
            else Just (x, if BS.null y then y else BS.tail y)

class ByteStringRepresentable a where
    toBs :: a -> BS.ByteString
    fromBs :: BS.ByteString -> (a, BS.ByteString)

instance ByteStringRepresentable BS.ByteString where
    toBs = id
    fromBs = id &&& const BS.empty

instance (Num a, Integral a, Bits a) => ByteStringRepresentable a where
    toBs bs = (BS.pack . map byteChar) steps
        where byteChar = chr . fromIntegral . ((2 ^ charBits - 1) .&.) . shiftR bs . (charBits *)
              steps = unfoldr (\n -> let n' = pred n in if n' < 0 then Nothing else Just (n', n')) (aBits `div` charBits)
              aBits = bitSize (undefined :: a)
    fromBs = ((const . BS.splitAt) (aBits `div` charBits) &&& id) >>> app >>> first fromBs'
        where fromBs' = BS.foldl' ((. fromIntegral . ord) . (+) . ((2 ^ charBits) *)) 0
              aBits = bitSize (undefined :: a)

data PacketMagicCode = Req | Res deriving (Show, Read, Eq)

packetMagicCodeBytes = 4

instance ByteStringRepresentable PacketMagicCode where
    toBs Req = BS.pack "\0REQ"
    toBs Res = BS.pack "\0RES"
    fromBs = BS.splitAt packetMagicCodeBytes >>> first fromBs'
        where fromBs' m | m == BS.pack "\0REQ" = Req
                        | m == BS.pack "\0RES" = Res
                        | otherwise = undefined

type PacketTypeId = Word32

packetTypeIdBytes = bitSize (undefined :: PacketTypeId) `div` charBits

type PacketMessageLength = Word32

packetMessageLengthBytes = bitSize (undefined :: PacketMessageLength) `div` charBits

data PacketMessage = PacketMessage [BS.ByteString] deriving (Show, Read)

instance ByteStringRepresentable PacketMessage where
    toBs (PacketMessage l) = toBs ((fromIntegral $ BS.length l') :: PacketMessageLength) `BS.append` l'
        where l' = BS.intercalate (BS.singleton '\0') l
    fromBs msg = (PacketMessage (explode ' ' msg), tail)
        where (len :: PacketMessageLength, rest) = fromBs msg
              (msg, tail) = BS.splitAt (fromIntegral len) rest

data Packet = Packet { magicCode :: PacketMagicCode, typeId :: PacketTypeId, message :: PacketMessage } deriving (Show, Read)

request :: PacketTypeId -> BS.ByteString -> BS.ByteString
request id msg = toBs Req `BS.append` toBs id `BS.append` toBs (PacketMessage [msg])

data LogMsgType = Debug | Info | ImportantInfo | Warning | Error | CriticalError | Crash deriving (Eq, Ord, Show)

class Logger a where
    (|<-) :: a -> String -> IO ()
    (|<-?) :: a -> (LogMsgType, String) -> IO ()
    logger |<-? (msgType, str) = logger |<- ("[" ++ (show msgType) ++ "] " ++ str)

instance Logger (String -> IO ()) where
    (|<-) = ($)

data BoxedLogger = forall a. Logger a => BoxedLogger a

instance Logger BoxedLogger where
    (BoxedLogger logger) |<- x = logger |<- x

class Instantiable a where
    new :: a

data ServerAddress = ServerAddress { serverAddress :: (N.HostName, N.PortID) } deriving Show

instance Instantiable ServerAddress where
    new = ServerAddress (undefined, undefined)

instance Wrapped ServerAddress (N.HostName, N.PortID) where
    wrap = ServerAddress
    unwrap = serverAddress

serverAddressHostName = accWrapped (accFirst :: Accessor (N.HostName, N.PortID) N.HostName)
serverAddressPort = accWrapped (accSecond :: Accessor (N.HostName, N.PortID) N.PortID)

data ServerSocket = ServerSocket { serverSocket :: (Handle, ServerAddress) } deriving Show

instance Instantiable ServerSocket where
    new = ServerSocket (undefined, new :: ServerAddress)

instance Wrapped ServerSocket (Handle, ServerAddress) where
    wrap = ServerSocket
    unwrap = serverSocket

serverSocketHandle = accWrapped (accFirst :: Accessor (Handle, ServerAddress) Handle)
serverSocketAddress = accWrapped (accSecond :: Accessor (Handle, ServerAddress) ServerAddress)
serverSocketHostName = serverSocketAddress .\ serverAddressHostName
serverSocketPort = serverSocketAddress .\ serverAddressPort

data Connection = Connection { connectionList :: [ServerSocket] }

instance Instantiable Connection where
    new = Connection []

instance Wrapped Connection [ServerSocket] where
    wrap = Connection
    unwrap = connectionList

connectionServerSocketList = accWrapped (accId :: Accessor [ServerSocket] [ServerSocket])
connectionNo = (connectionServerSocketList .\) . accListElt

type InvocationTimeout = Word32

data Invocation = Invocation { invocation :: (String, (BS.ByteString -> IO BS.ByteString, Maybe InvocationTimeout)) }

instance Instantiable Invocation where
    new = Invocation (undefined, undefined)

instance Wrapped Invocation (String, (BS.ByteString -> IO BS.ByteString, Maybe InvocationTimeout)) where
    wrap = Invocation
    unwrap = invocation

invocationName = accWrapped (accFirst :: Accessor (String, (BS.ByteString -> IO BS.ByteString, Maybe InvocationTimeout)) String)
invocationImplementation = accWrapped ((accSecond .\ accFirst) :: Accessor (String, (BS.ByteString -> IO BS.ByteString, Maybe InvocationTimeout)) (BS.ByteString -> IO BS.ByteString))
invocationTimeout = accWrapped ((accSecond .\ accSecond) :: Accessor (String, (BS.ByteString -> IO BS.ByteString, Maybe InvocationTimeout)) (Maybe InvocationTimeout))

data Service = Service { serviceMap :: M.Map String Invocation }

instance Instantiable Service where
    new = Service $ M.fromList []

instance Wrapped Service (M.Map String Invocation) where
    wrap = Service
    unwrap = serviceMap

serviceInvocationMap = accWrapped (accId :: Accessor (M.Map String Invocation) (M.Map String Invocation))
serviceName = (serviceInvocationMap .\) . accMapElt

type WorkerId = String

data WorkerState = WorkerState { workerState :: ((WorkerId, (Connection, Service)), BoxedLogger) }

instance Instantiable WorkerState where
    new = WorkerState ((undefined, (undefined, undefined)), undefined)

instance Wrapped WorkerState ((WorkerId, (Connection, Service)), BoxedLogger) where
    wrap = WorkerState
    unwrap = workerState

workerId = accWrapped ((accFirst .\ accFirst) :: Accessor ((WorkerId, (Connection, Service)), BoxedLogger) WorkerId)
workerConnection = accWrapped ((accFirst .\ accSecond .\ accFirst) :: Accessor ((WorkerId, (Connection, Service)), BoxedLogger) Connection)
workerService = accWrapped ((accFirst .\ accSecond .\ accSecond) :: Accessor ((WorkerId, (Connection, Service)), BoxedLogger) Service)
workerLogger = accWrapped (accSecond :: Accessor ((WorkerId, (Connection, Service)), BoxedLogger) BoxedLogger)

class WithLogger a where
    accLogger :: Accessor a BoxedLogger

instance WithLogger (a, BoxedLogger) where
    accLogger = accSecond :: Accessor (a, BoxedLogger) BoxedLogger

instance WithLogger WorkerState where
    accLogger = workerLogger

logstr msgType str = do
    l <- (~.>) accLogger
    liftIO (l |<-? (msgType, str))

logdbg = logstr Debug
loginfo = logstr Info
logwarn = logstr Warning
logerr = logstr Error

{- -}

serverList = [
    ServerAddress ("192.168.0.92", N.PortNumber 7003),
    ServerAddress ("192.168.0.92", N.PortNumber 7003)
    ]

serviceList = [
    Invocation ("id", ((\str -> (return . BS.pack . id . BS.unpack) str), Nothing)),
    Invocation ("reverse", ((\str -> (return . BS.pack . reverse . BS.unpack) str), Nothing))
    ]

initLogger = do
    let workerId = accFirst
    id <- (~.>) workerId
    accLogger ~.<< BoxedLogger (\x -> hPutStrLn stderr ("(" ++ id ++ ") " ++ x))

initConnection servers = do
    put new
    connectionServerSocketList ~.<< map ((new :: ServerSocket) .<< serverSocketAddress) servers

initService services = do
    put new
    mapM_ ((serviceInvocationMap ~.<) . uncurry M.insert . ((fst . (unwrap :: Invocation -> (String, (BS.ByteString -> IO BS.ByteString, Maybe InvocationTimeout)))) &&& id)) services

connect1 = do
    let socket = accFirst
    host <- (~.>) (socket .\ serverSocketHostName)
    port <- (~.>) (socket .\ serverSocketPort)
    logdbg $ "Connecting to " ++ (show host) ++ " (" ++ (show port) ++ ")."
    handle <- liftIO $ N.connectTo host port
    logdbg "Connected, setting buffering mode."
    liftIO $ hSetBuffering handle NoBuffering
    (socket .\ serverSocketHandle) ~.<< handle
    logdbg "All done."

connect = do
    let servers = accFirst
    logdbg "Connecting."
    connectionAccessors <- listAccessors (servers .\ connectionServerSocketList)
    mapM ((~.~ connect1) . (.* accLogger)) connectionAccessors
    logdbg "Connected."

send str = do
    let socket = accFirst
    h <- (~.>) (socket .\ serverSocketHandle)
    liftIO $ BS.hPut h str

flush = do
    let socket = accFirst
    h <- (~.>) (socket .\ serverSocketHandle)
    liftIO $ hFlush h

sendFlush str = do
    send str
    flush

register11 = do
    let invocation = accFirst .\ accFirst
    let socket = accFirst .\ accSecond
    function <- (~.>) (invocation .\ invocationName)
    logdbg ("Registering \"" ++ function ++ "\"")
    (socket .* accLogger) ~.~ (sendFlush (request 1 (BS.pack function)))
    logdbg ("Registered \"" ++ function ++ "\"")

register1 = do
    let socket = accFirst .\ accFirst
    let services = accFirst .\ accSecond
    socketInfo <- (~.>) socket
    logdbg ("Registering on " ++ (show socketInfo))
    serviceAccessors <- mapAccessors (services .\ serviceInvocationMap)
    mapM ((~.~ register11) . (.* accLogger) . (.* socket)) serviceAccessors
    logdbg ("Registered on " ++ (show socketInfo))

register = do
    logdbg "Registering."
    connectionAccessors <- listAccessors (workerConnection .\ connectionServerSocketList)
    mapM ((~.~ register1) . (.* accLogger) . (.* workerService)) connectionAccessors
    logdbg "Registered."

setId1 = do
    let socket = accFirst .\ accFirst
    let workerId = accFirst .\ accSecond
    socketInfo <- (~.>) socket
    workerIdStr <- (~.>) workerId
    logdbg $ "Setting ID on " ++ (show socketInfo)
    (socket .* accLogger) ~.~ (sendFlush (request 22 (BS.pack workerIdStr)))
    logdbg $ "ID set on " ++ (show socketInfo) ++ ": " ++ (show workerIdStr)

setId = do
    let connection = accFirst .\ accFirst
    let workerId = accFirst .\ accSecond
    logdbg "Setting ID."
    connectionAccessors <- listAccessors (connection .\ connectionServerSocketList)
    mapM ((~.~ setId1) . (.* accLogger) . (.* workerId)) connectionAccessors
    logdbg "ID set."

readPacketMagic = do
    let socket = accFirst
    h <- (~.>) (socket .\ serverSocketHandle)
    b <- liftIO $ BS.hGet h packetMagicCodeBytes
    (return . fst . fromBs) b

readPacketTypeId = do
    let socket = accFirst
    h <- (~.>) (socket .\ serverSocketHandle)
    b <- liftIO $ BS.hGet h packetTypeIdBytes
    (return . fst . fromBs) b

readPacketMessageLength = do
    let socket = accFirst
    h <- (~.>) (socket .\ serverSocketHandle)
    b <- liftIO $ BS.hGet h packetMessageLengthBytes
    (return . fst . fromBs) b

readPacketMessage = do
    let socket = accFirst
    (len :: PacketMessageLength) <- readPacketMessageLength
    h <- (~.>) (socket .\ serverSocketHandle)
    b <- liftIO $ BS.hGet h (fromIntegral len)
    return (PacketMessage [b])

readPacket = do
    magic <- readPacketMagic
    typeId <- readPacketTypeId
    message <- readPacketMessage
    return (Packet magic typeId message)

poll1 = do
    let socket = accFirst .\ accFirst
    let services = accFirst .\ accSecond
    socketInfo <- (~.>) socket
    logdbg $ "Polling " ++ (show socketInfo)
    (socket .* accLogger) ~.~ (sendFlush (request 9 BS.empty))
    resp <- (socket .* accLogger) ~.~ readPacket
    logdbg $ "Got back " ++ (show resp)

mainLoop = do
    logdbg "Twiddling thumbs."
    connectionAccessors <- listAccessors (workerConnection .\ connectionServerSocketList)
    mapM ((~.~ poll1) . (.* accLogger) . (.* workerService)) connectionAccessors
    logdbg "Once again from the top."
    mainLoop

work = do
    logdbg "Preparing for work."
    register
    (workerConnection .* workerId .* accLogger) ~.~ setId
    logdbg "Ready for work."
    logdbg "Going into the main cycle."
    mainLoop

worker servers services workerIdStr = (flip runStateT) (new :: WorkerState) $ do
    workerId ~.<< workerIdStr
    (workerId .* workerLogger) ~.~ initLogger
    workerConnection ~.~ (initConnection servers)
    workerService ~.~ (initService services)
    (workerConnection .* workerLogger) ~.~ connect
    work

t = worker serverList serviceList "gearmanworkerid0"

--main = withSocketsDo worker

