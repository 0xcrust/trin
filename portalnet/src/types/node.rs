use std::sync::Arc;

use ethportal_api::types::distance::Distance;
use ethportal_api::types::enr::Enr;

#[derive(Debug, Eq, PartialEq)]
/// A node in the overlay routing table.
pub struct Node {
    /// A shared reference to the node's ENR.
    pub enr: Arc<Enr>,
    /// The node's data radius
    pub data_radius: Distance,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            enr: Arc::clone(&self.enr),
            data_radius: self.data_radius,
        }
    }
}

impl Node {
    /// Creates a new node record
    pub fn new(enr: Enr, data_radius: Distance) -> Self {
        Node {
            enr: Arc::new(enr),
            data_radius,
        }
    }

    /// Creates a new node record from a shared instance
    pub fn new_from_arc(enr: Arc<Enr>, data_radius: Distance) -> Self {
        Node { enr, data_radius }
    }

    /// Returns a reference to the Enr of a node.
    pub fn enr(&self) -> &Enr {
        &self.enr
    }

    /// Returns the data radius of the node.
    pub fn data_radius(&self) -> Distance {
        self.data_radius
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node(node_id={}, radius={})",
            self.enr.node_id(),
            self.data_radius,
        )
    }
}
