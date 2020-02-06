(ns grubmaster.core
  (:require [grubmaster.graph :refer :all]
            [grubmaster.node :refer :all]))

;(def graph (-> (create-graph)
;               (add-node (create-map-node
;                           {:id 1 :transformer '(fn [x] (inc x)) :url "localhost" :port 8080}))
;               (add-node (create-map-node
;                           {:id 2 :transformer '(fn [x] (* x x)) :url "localhost" :port 8080}))
;               (add-link {:src :vent :dst 1})
;               (add-link {:src 1 :dst 2})
;               (add-link {:src 2 :dst :sink})))

(def graph (-> (create-graph)
               (add-node (create-map-node
                           {:id 1 :transformer '(fn [x] (inc x)) :url "localhost" :port 8080}))
               (add-node (create-map-node
                           {:id 2 :transformer '(fn [x] (* x x)) :url "localhost" :port 8080}))
               (add-node (create-reduce-node
                           {:id 3 :reducer '(fn [acc, x] (+ (or acc 0) x)) :url "localhost" :port 8080}))
               (add-link {:src :vent :dst 1})
               (add-link {:src 1 :dst 2})
               (add-link {:src 2 :dst 3})
               (add-link {:src 3 :dst :sink})))

(defn -main
  [& args]
  (prn "Hello World"))