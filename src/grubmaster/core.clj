(ns grubmaster.core
  (:require [grubmaster.graph :refer :all]
            [grubmaster.node :refer :all]))

;(def graph (-> (create-graph)
;               (add-node (create-map-node
;                           {:id 1 :mapperf '(fn [x] (inc x))}))
;               (add-node (create-map-node
;                           {:id 2 :mapperf '(fn [x] (* x x))}))
;               (add-node (create-reduce-node
;                           {:id 3 :reducerf '(fn [acc, x] (+ (or acc 0) x))}))
;               (add-link {:src :vent :dst 1})
;               (add-link {:src 1 :dst 2})
;               (add-link {:src 2 :dst 3})
;               (add-link {:src 3 :dst :sink})))

(def graph (-> (create-graph)
               (add-node (create-shell-node
                           {:id 1 :script (slurp "resources/test.bash")}))
               (add-link {:src :vent :dst 1})
               (add-link {:src 1 :dst :sink})))

(defn -main
  [& args]
  (prn "Hello World"))