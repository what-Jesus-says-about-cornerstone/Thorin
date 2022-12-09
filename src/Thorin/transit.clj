(ns Thorin.transit
  (:refer-clojure :exclude [read-string])
  (:require
   [clojure.string]
   [cognitect.transit :as transit]
   [Thorin.bytes])
  (:import (java.io ByteArrayOutputStream ByteArrayInputStream)))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn write-to-byte-array ^bytes
  [data type-kw opts]
  (with-open [out (ByteArrayOutputStream.)]
    (let [writer (transit/writer out type-kw opts)]
      (transit/write writer data)
      (.toByteArray out))))

(defn write-to-string ^String
  [data type-kw opts]
  (->
   (write-to-byte-array data type-kw opts)
   (Thorin.bytes/to-string)))

(defn read-byte-array
  [^bytes byte-arr type-kw opts]
  (with-open [in (ByteArrayInputStream. byte-arr)]
    (let [reader (transit/reader in type-kw opts)]
      (transit/read reader))))

(defn read-string
  [^String string type-kw opts]
  (->
   (Thorin.bytes/to-byte-array string)
   (read-byte-array type-kw opts)))

(comment
  
  
   clj -Sdeps '{:deps {org.clojure/clojure {:mvn/version "1.10.3"}
                       org.clojure/core.async {:mvn/version "1.3.618"}
                       expanse/bytes-jvm {:local/root "./expanse/bytes-jvm"}
                       expanse/transit-jvm {:local/root "./expanse/transit-jvm"}
                       com.cognitect/transit-clj {:mvn/version "1.0.324"}}}'
  
  clj -Sdeps '{:deps {org.clojure/clojurescript {:mvn/version "1.10.844"}
                      org.clojure/core.async {:mvn/version "1.3.618"}
                      expanse/bytes-js {:local/root "./expanse/bytes-js"}
                      expanse/bytes-meta {:local/root "./expanse/bytes-meta"}
                      expanse/transit-js {:local/root "./expanse/transit-js"}
                      com.cognitect/transit-cljs {:mvn/version "0.8.269"}}}' \
   -M -m cljs.main --repl-env node --compile expanse.transit.core --repl
  
  (do
    (require '[clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                                pub sub unsub mult tap untap mix admix unmix pipe
                                                timeout to-chan  sliding-buffer dropping-buffer
                                                pipeline pipeline-async]])
    
    (require '[Thorin.bytes] :reload)
    (require '[expanse.transit.core :as transit.core] :reload))
  
  
  ;
  )



(comment


  (defprotocol IWriter
    (write-byte-array* [_ data])
    (write-string* [_ data]))

  (defprotocol IReader
    (read-byte-array* [_ byte-arr])
    (read-string* [_ string]))

  (:clj
   (do
     (deftype TWriter [^cognitect.transit.Writer writer
                       ^ByteArrayOutputStream out]
       IWriter
       (write-byte-array*
         [_ data]
         (transit/write writer data)
         (let [byte-arr (.toByteArray out)]
           (.reset out)
           byte-arr))
       (write-string*
         [t data]
         (->
          (write-byte-array* t data)
          (Thorin.bytes/to-string))))

     (defn writer
       ([type-kw]
        (writer  type-kw {}))
       ([type-kw opts]
        (let [out (ByteArrayOutputStream.)]
          (writer  type-kw {} out)))
       ([type-kw opts out]
        (TWriter.
         (transit/writer out type-kw opts)
         out)))

     (deftype TReader [^cognitect.transit.Reader reader]
       IReader
       (read-byte-array*
         [_ byte-arr]
         (with-open [in (ByteArrayInputStream. byte-arr)])))))

  (:cljs
   (do
     (deftype TWriter [writer]
       IWriter
       (write-byte-array*
         [t data]
         (->
          (write-string* t data)
          (Thorin.bytes/to-byte-array)))
       (write-string*
         [_ data]
         (transit/write writer data)))

     (defn writer
       ([type-kw]
        (writer  type-kw nil))
       ([type-kw opts]
        (TWriter.
         (transit/writer type-kw opts))))))



  (defn write-byte-array
    [writer data]
    (write-byte-array* writer data))

  (defn write-string
    [writer data]
    (write-string* writer data))



  ;
  )