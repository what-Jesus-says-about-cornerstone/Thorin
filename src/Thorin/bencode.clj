(ns Thorin.bencode
  (:require
   [Thorin.protocols]
   [Thorin.bytes]
   [Thorin.expanse]))

(def ^:const colon-byte 58 #_(Thorin.expanse/char-code \:))
(def ^:const i-byte 105 #_(Thorin.expanse/char-code \i))
(def ^:const e-byte 101 #_(Thorin.expanse/char-code \e))
(def ^:const l-byte 108 #_(Thorin.expanse/char-code \l))
(def ^:const d-byte 100 #_(Thorin.expanse/char-code \d))

(defmulti encode*
  (fn
    ([data out]
     (cond
       (Thorin.bytes/byte-array? data) :byte-array
       (number? data) :number
       (string? data) :string
       (keyword? data) :keyword
       (map? data) :map
       (sequential? data) :sequential))
    ([data out dispatch-val]
     dispatch-val)))

(defmethod encode* :number
  [number out]
  (Thorin.protocols/write-byte* out i-byte)
  (Thorin.protocols/write-byte-array* out (Thorin.bytes/to-byte-array (str number)))
  (Thorin.protocols/write-byte* out e-byte))

(defmethod encode* :string
  [string out]
  (encode* (Thorin.bytes/to-byte-array string) out))

(defmethod encode* :keyword
  [kword out]
  (encode* (Thorin.bytes/to-byte-array (name kword)) out))

(defmethod encode* :sequential
  [coll out]
  (Thorin.protocols/write-byte* out l-byte)
  (doseq [item coll]
    (encode* item out))
  (Thorin.protocols/write-byte* out e-byte))

(defmethod encode* :map
  [mp out]
  (Thorin.protocols/write-byte* out d-byte)
  (doseq [[k v] (into (sorted-map) mp)]
    (encode* k out)
    (encode* v out))
  (Thorin.protocols/write-byte* out e-byte))

(defmethod encode* :byte-array
  [byte-arr out]
  (Thorin.protocols/write-byte-array* out (-> byte-arr (Thorin.bytes/alength) (str) (Thorin.bytes/to-byte-array)))
  (Thorin.protocols/write-byte* out colon-byte)
  (Thorin.protocols/write-byte-array* out byte-arr))

(defn encode
  "Takes clojure data, returns byte array"
  [data]
  (let [out (Thorin.bytes/byte-array-output-stream)]
    (encode* data out)
    (Thorin.protocols/to-byte-array* out)))

(defn peek-next
  [in]
  (let [byte (Thorin.protocols/read* in)]
    (when (== -1 byte)
      (throw (ex-info (str :decode* " unexpected end of InputStream") {})))
    (Thorin.protocols/unread* in byte)
    byte))

(defmulti decode*
  (fn
    ([in out]
     (condp = (peek-next in)
       i-byte :integer
       l-byte :list
       d-byte :dictionary
       :else :byte-array))
    ([in out dispatch-val]
     dispatch-val)))

(defmethod decode* :dictionary
  [in
   out
   & args]
  (Thorin.protocols/read* in) ; skip d char
  (loop [result (transient [])]
    (let [byte (peek-next in)]
      (cond

        (== byte e-byte) ; return
        (do
          (Thorin.protocols/read* in) ; skip e char
          (Thorin.protocols/reset* out)
          (apply hash-map (persistent! result)))

        (== byte i-byte)
        (if (even? (count result))
          (ex-info (str :decode*-dictionary " bencode keys must be strings, got integer") {})
          (recur (conj! result  (decode* in out :integer))))

        (== byte d-byte)
        (if (even? (count result))
          (ex-info (str :decode*-dictionary " bencode keys must be strings, got dictionary") {})
          (recur (conj! result  (decode* in out :dictionary))))

        (== byte l-byte)
        (if (even? (count result))
          (ex-info (str :decode*-dictionary " bencode keys must be strings, got list") {})
          (recur (conj! result  (decode* in out :list))))

        :else
        (let [byte-arr (decode* in out :byte-array)
              next-element (if (even? (count result))
                             #_its_a_key
                             (Thorin.bytes/to-string byte-arr)
                             #_its_a_value
                             byte-arr)]
          (recur (conj! result next-element)))))))

(defmethod decode* :list
  [in
   out
   & args]
  (Thorin.protocols/read* in) ; skip l char
  (loop [result (transient [])]
    (let [byte (peek-next in)]
      (cond

        (== byte e-byte) ; return
        (do
          (Thorin.protocols/read* in) ; skip e char
          (Thorin.protocols/reset* out)
          (persistent! result))

        (== byte i-byte)
        (recur (conj! result (decode* in out :integer)))

        (== byte d-byte)
        (recur (conj! result  (decode* in out :dictionary)))

        (== byte l-byte)
        (recur (conj! result  (decode* in out :list)))

        :else
        (recur (conj! result (decode* in out :byte-array)))))))

(defmethod decode* :integer
  [in
   out
   & args]
  (Thorin.protocols/read* in) ; skip i char
  (loop []
    (let [byte (Thorin.protocols/read* in)]
      (cond

        (== byte e-byte)
        (let [number-string (->
                             (Thorin.protocols/to-byte-array* out)
                             (Thorin.bytes/to-string))
              number (try
                       (Integer/parseInt number-string)
                       (catch
                        Exception
                        error
                         (Double/parseDouble number-string)))]
          (Thorin.protocols/reset* out)
          number)

        :else (do
                (Thorin.protocols/write-byte* out byte)
                (recur))))))

(defmethod decode* :byte-array
  [in
   out
   & args]
  (loop []
    (let [byte (Thorin.protocols/read* in)]
      (cond

        (== byte colon-byte)
        (let [length (-> (Thorin.protocols/to-byte-array* out)
                         (Thorin.bytes/to-string)
                         (Integer/parseInt))
              byte-arr (Thorin.protocols/read* in length)]
          (Thorin.protocols/reset* out)
          byte-arr)

        :else (do
                (Thorin.protocols/write-byte* out byte)
                (recur))))))

(defn decode
  "Takes byte array, returns clojure data"
  [byte-arr]
  (let [in (Thorin.bytes/pushback-input-stream byte-arr)
        out (Thorin.bytes/byte-array-output-stream)]
    (decode* in out)))


(comment

  clj -Sdeps '{:deps {expanse.bittorrent/bencode {:local/root "./bittorrent/bencode"}
                      expanse/core-jvm {:local/root "./expanse/core-jvm"}
                      expanse/bytes-jvm {:local/root "./expanse/bytes-jvm"}
                      expanse/codec-jvm {:local/root "./expanse/codec-jvm"}}} '
  
  clj -Sdeps '{:deps {org.clojure/clojurescript {:mvn/version "1.10.844"}
                      expanse/bencode {:local/root "./expanse/bencode"}
                      expanse/core-js {:local/root "./expanse/expanse-js"}
                      expanse/bytes-js {:local/root "./expanse/bytes-js"}
                      expanse/codec-js {:local/root "./expanse/codec-js"}}} '\
  -M -m cljs.main \
  -co '{:npm-deps {"randombytes" "2.1.0"
                   "bitfield" "4.0.0"
                   "fs-extra" "9.1.0"}
        :install-deps true
        :repl-requires [[cljs.repl :refer-macros [source doc find-doc apropos dir pst]]
                        [cljs.pprint :refer [pprint] :refer-macros [pp]]]} '\
  --repl-env node --compile Thorin.bencode --repl

  (require
   '[Thorin.bencode]
   '[Thorin.expanse]
   '[Thorin.bytes]
   '[Thorin.codec]
   :reload #_:reload-all)

  (let [data
        {:t "aabbccdd"
         :a {"id" "197957dab1d2900c5f6d9178656d525e22e63300"}}
        #_{:t (Thorin.codec/hex-to-bytes "aabbccdd")
           :a {"id" (Thorin.codec/hex-to-bytes "197957dab1d2900c5f6d9178656d525e22e63300")}}]

    (->
     (Thorin.bencode/encode data)
     #_(Thorin.bytes/to-string)
     #_(Thorin.bytes/to-byte-array)
     (Thorin.bencode/decode)
     #_(-> (get-in ["a" "id"]))
     #_(Thorin.codec/hex-to-string)))

  (let [data
        {:msg_type 1
         :piece 0
         :total_size 3425}]
    (->
     (Thorin.bencode/encode data)
     (Thorin.bytes/to-string)
     (Thorin.bytes/to-byte-array)
     (Thorin.bencode/decode)
     (clojure.walk/keywordize-keys)))

  ;
  )


(comment

  (require
   '[clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                      pub sub unsub mult tap untap mix admix unmix pipe
                                      timeout to-chan  sliding-buffer dropping-buffer
                                      pipeline pipeline-async]]
   '[clojure.core.async.impl.protocols :refer [closed?]])

  (require
   '[Thorin.bencode]
   '[Thorin.expanse]
   '[Thorin.bytes]
   '[Thorin.codec]
   :reload #_:reload-all)

  (defn foo
    [encode decode data]
    (let [timeout| (timeout 20000)]
      (go
        (loop []
          (when-not (closed? timeout|)
            (<! (timeout 10))
            (let [data data]
              (dotimes [i 50]
                (->
                 (encode data)
                 (decode)
                 (encode)
                 (decode))))
            (recur)))
        (println :done))))

  (let [data {:t (Thorin.codec/hex-to-bytes "aabbccdd")
              :a {"id" (Thorin.codec/hex-to-bytes "197957dab1d2900c5f6d9178656d525e22e63300")}}]
    (foo Thorin.bencode/encode Thorin.bencode/decode data))

  ; ~ 50% cpu node
  ; ~ 22% cpu jvm

  (def bencode (js/require "bencode"))
  
  (let [data {:t (js/Buffer.from "aabbccdd" "hex") 
              :a {"id" (js/Buffer.from "197957dab1d2900c5f6d9178656d525e22e63300" "hex")}}]
    (foo bencode.encode bencode.decode data))

  ; ~50% cpu


  ;
  )