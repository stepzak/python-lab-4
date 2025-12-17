from src.app_logger import AppLogger
from src.simulation import LibrarySimulation
import src.constants as cst

def main() -> None:
    """
    Обязательнная составляющая программ, которые сдаются. Является точкой входа в приложение
    :return: Данная функция ничего не возвращает
    """
    simulation = LibrarySimulation()
    AppLogger.configure_logger()
    print(simulation.run_simulation(cst.STEPS, cst.SEED))


if __name__ == "__main__":
    main()
