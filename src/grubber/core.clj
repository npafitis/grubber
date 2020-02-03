(ns grubber.core
  (:require [grubber.node :refer :all]
            [ring.middleware.content-type :refer :all]
            [ring.middleware.edn :as edn]
            [compojure.core :refer :all]
            [ring.adapter.jetty :refer :all]
            [ring.middleware.json :refer :all]
            [utils.core :as utils]
            [zeromq.zmq :as zmq]))

(def zmq-context (atom (zmq/context 1)))

(def content-type-value {:edn "application/edn" :json "application/json"})

(def status-value {:ok 200 :not-found 404})

(defn generate-body [data content-type]
  (cond (= content-type :edn) (pr-str data)
        :else data))

(defn generate-response [data & {:keys [status content-type]
                                 :or   {status :ok content-type :edn}}]
  {:status  (status-value status)
   :headers {"Content-Type" (content-type-value content-type)}
   :body    (generate-body data content-type)})

(defn grubber-handler [name node]
  ;; (run-grubber! node zmq-context)
  (let [grubber-port (run-grubber! node zmq-context)]
    (generate-response {:grubber-port grubber-port} :content-type :edn)))

(defroutes handler
           (POST "/" [name node]
             (grubber-handler name node)))

(def app
  (-> handler
      wrap-content-type
      edn/wrap-edn-params
      wrap-json-response))


(defn -main
  [& args]
  (run-jetty #'app {:port 8080}))