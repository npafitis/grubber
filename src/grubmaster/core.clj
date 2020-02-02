(ns grubmaster.core
  (:require [grubmaster.graph :refer :all]
            [grubmaster.node :refer :all]))

(def graph (-> (create-graph)
               (add-node (create-map-node
                           {:id 1 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 2 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 3 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 4 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 5 :transformer nil :url "localhost" :port 8080}))
               (add-link {:src :vent :dst 1})
               (add-link {:src 2 :dst 5})
               (add-link {:src 1 :dst 2})
               (add-link {:src 1 :dst 3})
               (add-link {:src 5 :dst 4})
               (add-link {:src 4 :dst 5})
               (add-link {:src 4 :dst :sink})
               (deploy!)))

(defn -main
  [& args]
  (prn "Hello World"))