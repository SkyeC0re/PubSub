use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct TopicSpecifiers {
    pub topics: Vec<TopicSpecifier>,
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum TopicSpecifier {
    #[serde(rename = "*")]
    Wildcard,
    #[serde(rename = ".")]
    ThisTopic,
    #[serde(rename = "st")]
    Subtopic {
        #[serde(rename = "t")]
        topic: String,
        #[serde(rename = "spec")]
        specifier: Box<TopicSpecifier>,
    },
}

impl TopicSpecifier {
    pub fn build() -> TopicSpecifierBuilder {
        TopicSpecifierBuilder::new()
    }
}

#[derive(Default)]
pub struct TopicSpecifierBuilder {
    topics: Vec<String>,
}

impl TopicSpecifierBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn subtopic<T: Into<String>>(mut self, subtopic: T) -> Self {
        self.topics.push(subtopic.into());
        self
    }

    pub fn this_topic(self) -> TopicSpecifier {
        self.terminate(TopicSpecifier::ThisTopic)
    }

    pub fn all(self) -> TopicSpecifier {
        self.terminate(TopicSpecifier::Wildcard)
    }

    pub fn terminate(self, terminator: TopicSpecifier) -> TopicSpecifier {
        self.topics
            .into_iter()
            .rev()
            .fold(terminator, |tail, topic| TopicSpecifier::Subtopic {
                topic,
                specifier: Box::new(tail),
            })
    }
}
