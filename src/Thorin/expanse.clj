(ns Thorin.expanse)

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn char-code ^Integer
  [^Character chr]
  (int chr))