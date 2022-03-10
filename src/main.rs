use actix_web::{get, middleware::Logger, post, web, App, HttpResponse, HttpServer, Responder};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    producer::{BaseRecord, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext, Message,
};
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use serde_json::json;
use log::debug;
use log::error;
use log::info;

use reqwest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[macro_use]
extern crate dotenv_codegen;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Measurements {
    length: usize,
    breadth: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderDetails {
    clothingtype: String,
    quantity: String,
    measurement: Measurements,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Order {
    orderid: String,
    resellerid: String,
    payment_status: String,
    payment_amount: String,
    details: OrderDetails,
    timestamp: String,
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[get("/getorders")]
async fn get_orders() -> impl Responder {
   
    let apikey: &str = dotenv!("supabase_apikey");
   
    let url: &str = dotenv!("table_url");

    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}?select=*", url).as_str())
        .header("apikey", apikey)
        .header("Authorization", format!("Bearer {}", apikey))
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    let res_json = serde_json::from_str::<serde_json::Value>(body.as_str()).unwrap();

    HttpResponse::Ok().json(res_json)
}

#[post("/placeorder")]
async fn echo(order: web::Json<Order>) -> impl Responder {
    let producer: ThreadedProducer<ProduceCallbackLogger> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        //for auth
        /*.set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")*/
        .create_with_context(ProduceCallbackLogger {})
        .expect("invalid producer config");

    let order_json = serde_json::to_string_pretty(&order).expect("json serialization failed");
    producer
        .send(
            BaseRecord::to("orders")
                .key(&format!("key-{}", order.orderid))
                .payload(&order_json),
        )
        .expect("Failed to place order");

    HttpResponse::Ok().body("Order place order successfully")
}

fn push_to_db(rx: Receiver<Order>) {
    info!("Strating Db service");
    let client = reqwest::blocking::Client::new();
    let apikey: &str = dotenv!("supabase_apikey");
    let url: &str = dotenv!("table_url");

    let mut map = HashMap::new();

    for val in rx {
        map.insert("orderid", val.orderid);
        map.insert("resellerid", val.resellerid);
        map.insert("payment_status", val.payment_status);
        map.insert("payment_amount", val.payment_amount);
        map.insert("clothing_type", val.details.clothingtype);
        map.insert("quantity", val.details.quantity);
        map.insert("measure_length", val.details.measurement.length.to_string());
        map.insert(
            "measure_breadth",
            val.details.measurement.breadth.to_string(),
        );

        map.insert("timestamp", val.timestamp);
        let res = client
            .post(url)
            .header("apikey", apikey)
            .header("Authorization", format!("Bearer {}", apikey))
            .header("Content-Type", "application/json")
            .header("Prefer", "return=representation")
            .json(&map)
            .send();
        println!("{:?}", res);
        map.clear();
    }
}

fn startlistener(tx: Sender<Order>) {
    info!("Starting Listener Thread");
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        //for auth
        /*.set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")*/
        .set("group.id", "my_consumer_group")
        .create()
        .expect("invalid consumer config");

    consumer
        .subscribe(&["orders"])
        .expect("topic subscribe failed");

    for msg_result in consumer.into_iter() {
        let msg = msg_result.unwrap();
        let _key: &str = msg.key_view().unwrap().unwrap();
        let value = msg.payload().unwrap();
        let order: Order =
            serde_json::from_slice(value).expect("failed to deserialize JSON to Order");
        tx.send(order);
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let (tx, rx) = channel::<Order>();

    std::thread::spawn(move || {
        startlistener(tx);
    });
    std::thread::spawn(move || {
        push_to_db(rx);
    });

    info!("Starting Server");

    HttpServer::new(|| {
        App::new()
            .wrap(Logger::new("%{r}a %r %{User-Agent}i"))
            .service(hello)
            .service(echo)
            .service(get_orders)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

struct ProduceCallbackLogger;

impl ClientContext for ProduceCallbackLogger {}

impl ProducerContext for ProduceCallbackLogger {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let dr = delivery_result.as_ref();
        //let msg = dr.unwrap();

        match dr {
            Ok(msg) => {
                let key: &str = msg.key_view().unwrap().unwrap();
                info!(
                    "produced message with key {} in offset {} of partition {}",
                    key,
                    msg.offset(),
                    msg.partition()
                )
            }
            Err(producer_err) => {
                let key: &str = producer_err.1.key_view().unwrap().unwrap();

                error!(
                    "failed to produce message with key {} - {}",
                    key, producer_err.0,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    // #[tokio::test]
    // async fn test_single_return() -> Result<(), Box<dyn std::error::Error>> {
    //     let body = json!({
    //         "query": "Professor",
    //         "name": [],
    //         "city": ["Xin’an"],
    //         "country": ["China"],
    //         "state": ["nework"],
    //         "business_Sector": [],
    //         "business_type": []
    //     });

    //     let data="[{\"id\":\"308\",\"organizationid\":\"b8957b97-c95c-4163-9f37-e2ae9059d0ac\",\"name\":\"Quaxo\",\"city\":\"Xin’an\",\"country\":\"China\",\"state\":\"nework\",\"zipcode\":\"43547-321\",\"contact\":\"427-106-3080\",\"url\":\"http://aol.com/vitae/nisl/aenean/lectus/pellentesque/eget.jpg?eget=habitasse&congue=platea&eget=dictumst&semper=etiam&rutrum=faucibus&nulla=cursus&nunc=urna&purus=ut&phasellus=tellus&in=nulla&felis=ut&donec=erat&semper=id&sapien=mauris&a=vulputate&libero=elementum&nam=nullam&dui=varius&proin=nulla&leo=facilisi&odio=cras&porttitor=non&id=velit&consequat=nec&in=nisi&consequat=vulputate&ut=nonummy&nulla=maecenas&sed=tincidunt&accumsan=lacus&felis=at&ut=velit&at=vivamus&dolor=vel&quis=nulla&odio=eget&consequat=eros&varius=elementum&integer=pellentesque&ac=quisque&leo=porta&pellentesque=volutpat&ultrices=erat&mattis=quisque&odio=erat&donec=eros&vitae=viverra&nisi=eget\",\"regionid\":32,\"internship_role\":\"Professor\",\"business_sector\":\"Major Pharmaceuticals\",\"business_type\":\"Research and Development\",\"company_size\":49265,\"is_verified\":true,\"is_active\":false}]";

    //     let req_url = "http://localhost:3030/search";

    //     let response = reqwest::Client::new()
    //         .post(req_url)
    //         .json(&body)
    //         .send()
    //         .await?;
    //     let gist = response.text().await?;
    //     //    let body_text=gist.to_string();
    //     assert_eq!(data, gist);
    //     Ok(())
    // }

    #[tokio::test]
    async fn test_getorders() -> Result<(), Box<dyn std::error::Error>> {

        let data = r#"[{"clothing_type":"Shirt","measure_breadth":"28","measure_length":"32","orderid":"YYOrd-40d37599-d451-4c6a-8f9f-503fa01b613b","payment_amount":"10000","payment_status":"paid","quantity":"18","resellerid":"Rs-da5b4042-adc9-43e5-b836-0b4121c1cbde","timestamp":"1646337578.4699323"},{"clothing_type":"Shirt","measure_breadth":"28","measure_length":"32","orderid":"Ord-40d37599-d451-4c6a-8f9f-503fa01b613b","payment_amount":"10000","payment_status":"paid","quantity":"18","resellerid":"Rs-da5b4042-adc9-43e5-b836-0b4121c1cbde","timestamp":"1646337578.4699323"},{"clothing_type":"Shirt","measure_breadth":"28","measure_length":"32","orderid":"LLOrd-40d37599-d451-4c6a-8f9f-503fa01b613b","payment_amount":"10000","payment_status":"paid","quantity":"18","resellerid":"Rs-da5b4042-adc9-43e5-b836-0b4121c1cbde","timestamp":"1646337578.4699323"},{"clothing_type":"Shirt","measure_breadth":"28","measure_length":"32","orderid":"LplOrd-40d37599-d451-4c6a-8f9f-503fa01b613b","payment_amount":"10000","payment_status":"paid","quantity":"18","resellerid":"Rs-da5b4042-adc9-43e5-b836-0b4121c1cbde","timestamp":"1646337578.4699323"},{"clothing_type":"Shirt","measure_breadth":"28","measure_length":"32","orderid":"LoplsalOrd-40d37599-d451-4c6a-8f9f-503fa01b613b","payment_amount":"10000","payment_status":"paid","quantity":"18","resellerid":"Rs-da5b4042-adc9-43e5-b836-0b4121c1cbde","timestamp":"1646337578.4699323"},{"clothing_type":"Shirt","measure_breadth":"28","measure_length":"32","orderid":"NormalOrd-40d37599-d451-4c6a-8f9f-503fa01b613b","payment_amount":"10000","payment_status":"paid","quantity":"18","resellerid":"Rs-da5b4042-adc9-43e5-b836-0b4121c1cbde","timestamp":"1646337578.4699323"}]"#;

        let req_url = "http://localhost:8080/getorders";

        let response = reqwest::Client::new()
            .get(req_url)
            .send()
            .await?;
        let gist = response.text().await?;
        //    let body_text=gist.to_string();
        assert_eq!(data, gist);
        Ok(())
    }

    #[tokio::test]
    async fn test_placeorder_success() -> Result<(), Box<dyn std::error::Error>> {
        
        let body = json!({
            "orderid": "NormalOrd-40d37599-d451-4c6a-8f9f-503fa01b613b",
            "resellerid": "Rs-da5b4042-adc9-43e5-b836-0b4121c1cbde",
            "payment_status": "paid",
            "payment_amount": "10000",
            "details": {    
                "clothingtype": "Shirt",
                "quantity": "18",
                "measurement": 
                    {
                        "length": 32,
                        "breadth": 28
                    }
                                
            },
            "timestamp": "1646337578.4699323"
        });

        let data="Order place order successfully";

        let req_url = "http://localhost:8080/placeorder";

        let response = reqwest::Client::new()
            .post(req_url)
            .json(&body)
            .send()
            .await?;
        let gist = response.text().await?;
        //    let body_text=gist.to_string();
        assert_eq!(data, gist);
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_body() -> Result<(), Box<dyn std::error::Error>> {
        let body = json!({});

        let data = "Json deserialize error: missing field `orderid` at line 1 column 2";

        let req_url = "http://localhost:8080/placeorder";

        let response = reqwest::Client::new()
            .post(req_url)
            .json(&body)
            .send()
            .await?;

        let gist = response.text().await?;
        //    let body_text=gist.to_string();
        assert_eq!(data, gist);
        Ok(())
    }
}
