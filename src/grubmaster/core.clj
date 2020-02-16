(ns grubmaster.core
  (:require [grubmaster.graph :refer :all]
            [grubmaster.node :refer :all]
            [clojure.edn :as edn]))


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
               (add-node (create-map-node
                           {:id 1 :mapperf '(fn [x] (inc x))}))
               (add-node (create-map-node
                           {:id 2 :mapperf '(fn [x] (* x x))}))
               (add-node (create-map-node
                           {:id 3 :mapperf '(fn [x] (* x x))}))
               (add-node (create-reduce-node
                           {:id 4 :reducerf '(fn [acc, x] (+ (or acc 0) x))}))
               (add-link {:src :vent :dst 1})
               (add-link {:src 1 :dst 2})
               (add-link {:src 1 :dst 3})
               (add-link {:src 2 :dst 4})
               (add-link {:src 3 :dst 4})
               (add-link {:src 4 :dst :sink})))

(def runner-key
  {:map    :mapperf
   :reduce :reducerf
   :scan   :scannerf
   :shell  :script})

(def node-constructor
  {:map    #'create-map-node
   :scan   #'create-scan-node
   :reduce #'create-reduce-node
   :shell  #'create-shell-node})

(defn construct-node [constructor node]
  (constructor node))

(defn parse-node-defs [graph config]
  (loop [graph graph
         nodes (:nodes config)]
    (let [node (first nodes)
          node-type (:type node)]
      (if (nil? node)
        graph
        (recur
          (add-node graph
                    (-> (construct-node (node-constructor node-type)
                                        {:id                    (:id node)
                                         (runner-key node-type) (or (:fn node) (:script node))})
                        (update-node-port (:port node))
                        (update-node-url (:url node))
                        (update-node-threads (:threads node))))
          (rest nodes))))))

(defn parse-link-defs [graph config]
  (loop [graph graph
         links (:links config)]
    (let [link (first links)]
      (if (nil? link)
        graph
        (recur
          (add-link graph
                    {:src (:src link)
                     :dst (:dst link)})
          (rest links))))))

(defn parse-grubfile [path]
  (let [config (edn/read-string (slurp path))]
    (-> (create-graph)
        (parse-node-defs config)
        (parse-link-defs config))))

(defn -main
  [& args]
  (prn "Hello World"))