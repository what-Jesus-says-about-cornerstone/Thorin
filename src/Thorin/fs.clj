(ns Thorin.fs
  (:refer-clojure :exclude [remove])
  (:require
   [clojure.java.io :as io]
   [Thorin.protocols])
  (:import (java.io Writer File)))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn path-join
  [& args]
  (->
   (.getCanonicalPath ^File (apply io/file args))))

(defn path-exists?
  [filepath]
  (.exists (io/file filepath)))

(defn read-file
  [filepath]
  (slurp filepath))

(defn write-file
  [filepath data-string]
  (spit filepath data-string))

(defn make-parents
  [filepath]
  (io/make-parents filepath))

(deftype TWriter [^Writer io-writer]
  Thorin.protocols/PWriter
  (write-string*
    [_ string]
    (.write io-writer ^String string))
  Thorin.protocols/Close
  (close*
    [_]
    (.close io-writer)))

(defn writer
  [x & opts]
  (let [io-writer (apply io/writer x opts)]
    (TWriter.
     io-writer)))

(defn remove
  ([filepath]
   (remove filepath true))
  ([filepath silently?]
   (io/delete-file filepath silently?)))