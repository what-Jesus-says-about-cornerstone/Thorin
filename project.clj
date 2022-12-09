(defproject program ""

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.5.648"]

                 [aleph/aleph "0.4.7-alpha5"]
                 [byte-streams/byte-streams "0.2.5-alpha2"]
                 [manifold/manifold "0.1.9-alpha3"]
                 [io.replikativ/datahike "0.5.1504"]

                 [com.formdev/flatlaf "2.1"]
                 [com.formdev/flatlaf-extras "2.1"]
                 [com.miglayout/miglayout-swing "5.3"]]

  :source-paths ["src"]
  :target-path "out"

  :main Thorin.main
  :profiles {:uberjar {:aot :all}}
  :repl-options {:init-ns Thorin.main}
  :jvm-opts ["-Dclojure.compiler.direct-linking=true"
             "-Dclojure.core.async.pool-size=8"]

  :uberjar-name "Thorin.jar")