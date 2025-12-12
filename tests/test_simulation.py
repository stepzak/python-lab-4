from src.simulation import LibrarySimulation, SimulationResults


def test_simulation():
    simulation = LibrarySimulation()
    results: list[SimulationResults] = []
    for _ in range(100):
        results.append(simulation.run_simulation(20, 52))
        if len(results)>=2:
            assert results[-1].result == results[-2].result