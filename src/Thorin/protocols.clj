(ns Thorin.protocols)

(defprotocol Close
  (close* [_]))

(defprotocol Send
  (send* [_ data] [_ data address]))

(defprotocol DatagramSocket
  (listen* [_])
  #_Close
  #_IDeref)

(defprotocol Socket
  (connect* [_])
  #_Send
  #_Close
  #_IDeref)

(defprotocol IPushbackInputStream
  (read* [_] [_ length])
  (unread* [_ char-int])
  (unread-byte-array* [_ byte-arr] [_ byte-arr offset length]))

(defprotocol IToByteArray
  (to-byte-array* [_]))

(defprotocol IByteArrayOutputStream
  (write-byte* [_ char-int])
  (write-byte-array* [_ byte-arr])
  (reset* [_])
  #_IToByteArray)

(defprotocol IBitSet
  (get* [_ bit-index])
  (get-subset* [_ from-index to-index])
  (set* [_ bit-index] [_ bit-index value])
  #_IToByteArray)

(defprotocol Closable
  (close [_]))

(defprotocol PWriter
  (write-string* [_ string])
  #_Close)

(defprotocol Wire
  #_IDeref)