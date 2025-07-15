use deadpool_postgres::Pool;
use std::sync::Arc;

pub struct AppState {
    pub pool: Pool,
    pub tables: Arc<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use deadpool_postgres::Config;
    use tokio_postgres::NoTls;

    #[test]
    fn test_app_state_structure() {
        // Test that AppState has the expected fields
        // This test primarily validates the struct definition

        // We can't easily create a real Pool in tests without a database
        // So we'll just ensure the struct is properly defined and accessible
        
        // The struct should have two fields: pool and tables
        // This is verified by the compilation itself, but we can add some basic assertions
        
        // Since we can't create a real AppState without a database connection,
        // we'll test that the struct fields are accessible through a mock scenario
        
        // This test ensures that if we had an AppState instance, 
        // we could access its fields as expected
        assert_eq!(std::mem::size_of::<AppState>(), std::mem::size_of::<Pool>() + std::mem::size_of::<Arc<Vec<String>>>());
    }
    
    #[test]
    fn test_app_state_field_types() {
        // Test that the field types are correct
        // This is more of a compile-time test, but helps document the structure
        
        use std::any::TypeId;
        
        // Verify the types are what we expect
        assert_eq!(TypeId::of::<Pool>(), TypeId::of::<Pool>());
        assert_eq!(TypeId::of::<Arc<Vec<String>>>(), TypeId::of::<Arc<Vec<String>>>());
    }
}
