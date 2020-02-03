(ns grubmaster.core
  (:require [grubmaster.graph :refer :all]
            [grubmaster.node :refer :all]))

(def graph (-> (create-graph (range 0 10000))
               (add-node (create-map-node
                           {:id 1 :transformer nil :url "localhost" :port 8080}))
               (add-node (create-map-node
                           {:id 2 :transformer nil :url "localhost" :port 8080}))
               (add-node (create-map-node
                           {:id 3 :transformer nil :url "localhost" :port 8080 :threads 4}))
               (add-link {:src :vent :dst 1})
               (add-link {:src 1 :dst 2})
               (add-link {:src 1 :dst 3})
               (add-link {:src 2 :dst 3})
               (add-link {:src 3 :dst :sink})))

(defn -main
  [& args]
  (prn "Hello World"))