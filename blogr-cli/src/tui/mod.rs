pub mod app;
pub mod config_app;
pub mod editor;
pub mod events;
pub mod markdown;
pub mod preview;
pub mod theme;

pub use app::{App, AppResult};
pub use events::{Event, EventHandler};

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    Terminal,
};
use std::io;

use crate::tui::config_app::ConfigApp;

/// Representation of a terminal user interface.
///
/// It is responsible for setting up the terminal,
/// initializing the interface and handling the draw events.
pub struct Tui<B: Backend> {
    /// Interface to the Terminal.
    terminal: Terminal<B>,
    /// Terminal event handler.
    pub events: EventHandler,
}

impl<B: Backend> Tui<B> {
    /// Constructs a new instance of [`Tui`].
    pub fn new(terminal: Terminal<B>, events: EventHandler) -> Self {
        Self { terminal, events }
    }

    /// Initializes the terminal interface.
    ///
    /// It enables the raw mode and sets terminal properties.
    pub fn init(&mut self) -> AppResult<()> {
        enable_raw_mode()?;
        execute!(io::stderr(), EnterAlternateScreen, EnableMouseCapture)?;

        // Define a custom panic hook to reset the terminal properties.
        // This way, you won't have your terminal messed up if an unexpected error happens.
        let panic_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic| {
            Self::reset().expect("failed to reset the terminal");
            panic_hook(panic);
        }));

        self.terminal.hide_cursor()?;
        self.terminal.clear()?;
        Ok(())
    }

    /// [`Draw`] the terminal interface by [`rendering`] the widgets.
    ///
    /// [`Draw`]: tui::Terminal::draw
    /// [`rendering`]: crate::ui:render
    pub fn draw(&mut self, app: &mut App) -> AppResult<()> {
        self.terminal.draw(|frame| app.render(frame))?;
        Ok(())
    }

    /// Draw the configuration app
    pub fn draw_config(&mut self, config_app: &mut ConfigApp) -> AppResult<()> {
        self.terminal.draw(|frame| config_app.render(frame))?;
        Ok(())
    }

    /// Draw the newsletter approval app
    pub fn draw_approval(
        &mut self,
        approval_app: &mut crate::newsletter::ModernApprovalApp,
    ) -> AppResult<()> {
        self.terminal.draw(|frame| approval_app.render(frame))?;
        Ok(())
    }

    /// Resets the terminal interface.
    ///
    /// This function is also used for the panic hook to revert
    /// the terminal properties if unexpected errors occur.
    fn reset() -> AppResult<()> {
        disable_raw_mode()?;
        execute!(io::stderr(), LeaveAlternateScreen, DisableMouseCapture)?;
        Ok(())
    }

    /// Exits the terminal interface.
    ///
    /// It disables the raw mode and reverts back the terminal properties.
    pub fn exit(&mut self) -> AppResult<()> {
        Self::reset()?;
        self.terminal.show_cursor()?;
        Ok(())
    }
}

/// Initialize the terminal user interface.
pub fn init() -> AppResult<Tui<CrosstermBackend<io::Stderr>>> {
    let backend = CrosstermBackend::new(io::stderr());
    let terminal = Terminal::new(backend)?;
    let events = EventHandler::new(250);
    Ok(Tui::new(terminal, events))
}

/// Initialize the terminal user interface with optimized settings for high-performance interfaces.
pub fn init_optimized() -> AppResult<Tui<CrosstermBackend<io::Stderr>>> {
    let backend = CrosstermBackend::new(io::stderr());
    let terminal = Terminal::new(backend)?;
    let events = EventHandler::new(16); // 60fps for smooth interactions
    Ok(Tui::new(terminal, events))
}
