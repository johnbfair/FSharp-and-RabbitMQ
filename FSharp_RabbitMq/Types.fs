namespace FSharp_RabbitMq.Types

open RabbitMQ.Client

type Agent<'T> = MailboxProcessor<'T>

type Credentials = { Host:string; Username:string; Password:string }

type Command = {Message:string; Tag:uint64}

type Status =
    | Start
    | Stop

type PublishType =
    | Exchange of string
    | Queue of string

// Sample classes (likely from OOP language like C#)
type Message1(sample1, value1, messageTag) = 
    member val Sample1 = sample1 with get,set
    member val Value1 = value1 with get,set
    member val MessageTag = messageTag with get,set
    new () = Message1("", 0, 0UL)

// Sample classes (likely from OOP language like C#)
type Message2(sample2, value2, messageTag) = 
    member val Sample2 = sample2 with get,set
    member val Value2 = value2 with get,set
    member val MessageTag = messageTag with get,set
    new () = Message2("", 0, 0UL)

module Agent =
    let reportErrorsTo (supervisor: Agent<exn>) (agent: Agent<_>) =
        agent.Error.Add(fun error -> supervisor.Post error); agent

    let start (agent: Agent<_>) = agent.Start(); agent

    let stop (agent: Agent<_>) = (agent :> System.IDisposable).Dispose()
        
type RabbitMqPublisher (creds, publishType, persist) = 
    let connectionFactory = lazy new ConnectionFactory(UserName=creds.Username, Password=creds.Password, Uri=creds.Host)
    let model =
        let connection = connectionFactory.Value.CreateConnection()
        let model = lazy connection.CreateModel()
        
        if (persist) then
        // Setting a channel into confirm mode by calling IModel.ConfirmSelect causes the broker to send a Basic.Ack 
        // after each message is processed by delivering to a ready consumer or by persisting to disk.
            model.Value.ConfirmSelect()
        // Enure it was either picked up or written to disk
            model.Value.WaitForConfirmsOrDie()
        
        model
        // Used when I was doing queue declare programmatically which I'm not anymore            
        //match publishType with
        //| Queue x -> m.QueueDeclare(x, true, false, false, null) |> ignore
        //| _ -> ()
    let properties = lazy model.Value.CreateBasicProperties(DeliveryMode=2uy) //2uy sets the message properties to Durable

    let sendMsg (msg:string) (exchange, routingKey) =
        async { 
            model.Value.BasicPublish(exchange, routingKey, properties.Value, System.Text.Encoding.ASCII.GetBytes msg)            
        }
    
    let receiveMessage f = new Events.BasicNackEventHandler(fun sender args -> f args.Requeue args.DeliveryTag)

    member this.BindNackEvent f = model.Value.add_BasicNacks(receiveMessage f)

    member this.Send (msg:string) (routingKey:string option) = 
        let send = sendMsg msg
        match publishType, routingKey with
        | Queue x, _         -> send ("", x)
        | Exchange x, None   -> send (x, "")
        | Exchange x, Some y -> send (x, y)

type RabbitMqSubscriber(creds, queue) =
    let connectionFactory = lazy new ConnectionFactory(UserName=creds.Username, Password=creds.Password, Uri=creds.Host)    
    let model = 
        let connection =  connectionFactory.Value.CreateConnection()
        lazy connection.CreateModel()
        // Used when I was doing queue declare programmatically which I'm not anymore            
        //m.QueueDeclare(queue, true, false, false, null) |> ignore 
    let receiveMessage f = new Events.BasicDeliverEventHandler(fun sender args -> 
        f (System.Text.Encoding.ASCII.GetString args.Body) args.DeliveryTag)
    let consumer = new Events.EventingBasicConsumer(Model=model.Value)

    member this.BindReceivedEvent f = consumer.add_Received(receiveMessage f)
    member this.Start = model.Value.BasicConsume(queue, false, consumer)
    member this.Working = consumer.IsRunning
    member this.AckMessage tag = model.Value.BasicAck(tag, false)