INSERT INTO Farmers (farmer_id, name, email, registration_date) VALUES
(1, 'John Smith', 'john.smith@farm.com', '2023-01-15'),
(2, 'Maria Garcia', 'maria.garcia@farm.com', '2023-02-20'),
(3, 'David Johnson', 'david.johnson@farm.com', '2023-03-10'),
(4, 'Sarah Wilson', 'sarah.wilson@farm.com', '2023-04-05'),
(5, 'Michael Brown', 'michael.brown@farm.com', '2023-05-01');

-- Insert Plots
INSERT INTO Plots (plot_id, farmer_id, location, area) VALUES
(1,1, 'North Field A1', 5.5),
(2,1, 'North Field A2', 4.2),
(3,2, 'South Valley B1', 6.0),
(4,3, 'East Ridge C1', 3.8),
(5,3, 'East Ridge C2', 4.5),
(6,4, 'West Plains D1', 7.2),
(7,5, 'Central Plot E1', 5.0);

-- Insert Crops (Using different planting seasons and crop types)
INSERT INTO Crops (crop_id, plot_id, crop_type, planting_date, expected_harvest_date) VALUES
(1,1, 'Wheat', '2024-03-15', '2024-08-15'),
(2,1, 'Soybeans', '2023-05-01', '2023-10-01'),
(3,2, 'Corn', '2024-04-01', '2024-09-01'),
(4,3, 'Rice', '2024-03-20', '2024-09-20'),
(5,4, 'Potatoes', '2024-04-15', '2024-08-15'),
(6,5, 'Tomatoes', '2024-05-01', '2024-08-30'),
(7,6, 'Cotton', '2024-04-10', '2024-10-10'),
(8,7, 'Sunflowers', '2024-05-15', '2024-09-15');

-- Insert Harvests (for completed harvests from previous season)
INSERT INTO Harvests (harvest_id, crop_id, harvest_date, yield_kg) VALUES
(1,2, '2023-09-28', 12500.75),
(2,2, '2023-10-05', 11800.50);