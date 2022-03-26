pub fn log(msg: String) {
    println!("{} - {}", chrono::offset::Local::now(), msg)
}