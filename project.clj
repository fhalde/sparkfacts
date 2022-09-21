(defproject sparkfacts "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :jvm-opts ["-Xmx3g"]
  :dependencies [[meander/epsilon "0.0.650"]
                 [org.clojure/clojure "1.10.3"]
                 [com.xtdb/xtdb-core "1.21.0"]
                 [com.xtdb/xtdb-rocksdb "1.21.0"]]
  :repl-options {:init-ns sparkfacts.core})
