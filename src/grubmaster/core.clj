(ns grubmaster.core
  (:require [grubmaster.graph :refer :all]
            [grubmaster.node :refer :all]))

(def graph (-> (create-graph)
               (add-node (create-map-node
                           {:id 1 :transformer '(fn [x] (inc x)) :url "localhost" :port 8080}))
               (add-node (create-map-node
                           {:id 2 :transformer '(fn [x] (* x x)) :url "localhost" :port 8080}))
               (add-link {:src :vent :dst 1})
               (add-link {:src 2 :dst :sink})))

(defn -main
  [& args]
  (prn "Hello World"))